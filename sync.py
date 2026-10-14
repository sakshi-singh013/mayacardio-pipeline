"""Edge Synchronization and Mode Switching Engine.

Manages:
- Network state detection and automatic mode switching (online <-> offline within 10s)
- Live streaming of data to cloud in online mode
- Local offline processing when network is unavailable
- Fast backlog catch-up synchronization upon network restoration
"""
import time
import json
import logging
import urllib.request
import urllib.error
from typing import Optional, Dict, Any, List

from config import CLOUD_URL, NETWORK_TIMEOUT_SECONDS, CATCHUP_BATCH_SIZE
from storage import PipelineStorage
from processor import process_window

logger = logging.getLogger("SyncEngine")


class SyncEngine:
    def __init__(self, storage: PipelineStorage, cloud_url: str = CLOUD_URL):
        self.storage = storage
        self.cloud_url = cloud_url.rstrip("/")
        self.mode = "offline"
        self.consecutive_failures = 0
        self.is_running = True
        self.last_offline_processed_window: Dict[str, int] = {}

    def check_network(self) -> bool:
        """Ping cloud server health endpoint."""
        url = f"{self.cloud_url}/health"
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=NETWORK_TIMEOUT_SECONDS) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        return False

    def push_batch_to_cloud(self, items: List[Dict[str, Any]]) -> bool:
        """Push batch of samples to cloud server."""
        if not items:
            return True
        
        url = f"{self.cloud_url}/api/stream"
        payload = json.dumps({"samples": items}).encode("utf-8")
        try:
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=NETWORK_TIMEOUT_SECONDS) as resp:
                if resp.status == 200:
                    self.storage.mark_synced(items)
                    return True
        except Exception as e:
            logger.warning(f"Failed to push batch to cloud: {e}")
        return False

    def update_mode(self):
        """Update current mode (online / offline) with fast 10s transition."""
        is_online = self.check_network()
        prev_mode = self.mode

        if is_online:
            self.consecutive_failures = 0
            self.mode = "online"
        else:
            self.consecutive_failures += 1
            # If 2 consecutive checks fail (~4-6s), switch to offline (spec requirement: <10s)
            if self.consecutive_failures >= 2:
                self.mode = "offline"

        if self.mode != prev_mode:
            logger.info(f"Mode switched: {prev_mode} -> {self.mode}")
            self.storage.set_meta("current_mode", self.mode)

    def process_offline_windows(self, session_id: str):
        """Perform Section 3 processing locally when network is down."""
        last_w = self.last_offline_processed_window.get(session_id, -1)
        
        status = self.storage.get_status_counts(session_id)
        if status["samples_saved"] < 400:
            return

        with self.storage._get_connection() as conn:
            row = conn.execute(
                "SELECT MAX(sample_index) as max_idx FROM samples WHERE session_id = ?;",
                (session_id,)
            ).fetchone()
            if not row or row["max_idx"] is None:
                return
            max_w = row["max_idx"] // 400

        for w in range(last_w + 1, max_w):
            start_i = 400 * w
            end_i = 400 * w + 399
            s1_data = self.storage.get_window_sensor_data(session_id, 1, start_i, end_i)
            s2_data = self.storage.get_window_sensor_data(session_id, 2, start_i, end_i)
            
            # Fetch S1 ch2 history for heart rate (needs 800 samples prior to window start)
            hr_start = max(0, 400 * (w - 6))
            hr_end = 400 * (w + 1)
            s1_ch2 = self.storage.get_sensor1_ch2_slice(session_id, hr_start, hr_end)

            result = process_window(
                session_id=session_id,
                window=w,
                mode="offline",
                s1_samples=s1_data,
                s2_samples=s2_data,
                s1_ch2_history=s1_ch2
            )
            self.storage.save_window_result(result)
            self.last_offline_processed_window[session_id] = w

    def sync_cycle(self):
        """Execute one tick of the synchronization loop."""
        self.update_mode()
        active_session = self.storage.get_meta("active_session_id")

        if self.mode == "online":
            # Stream backlog / unsynced samples
            while True:
                batch = self.storage.get_unsynced_batch(limit=CATCHUP_BATCH_SIZE)
                if not batch:
                    break
                if not self.push_batch_to_cloud(batch):
                    # Network dropped during upload
                    self.mode = "offline"
                    break
        else:
            # Offline mode: process completed windows locally on the device
            if active_session:
                self.process_offline_windows(active_session)
