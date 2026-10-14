"""Edge collector reading biosignal-mock continuously over TCP.

Features:
- Robust socket connection with auto-reconnect
- Strict CRC-32 verification of data payloads
- Automatic re-read on payload corruption
- Session-restart detection and incomplete session loss tracking
- Immediate ACID persistence to local SQLite storage
"""
import socket
import json
import zlib
import time
import logging
from typing import Optional, Dict, Any, Tuple
from config import MOCK_HOST, MOCK_PORT
from storage import PipelineStorage

logger = logging.getLogger("Collector")


class MockClient:
    def __init__(self, storage: PipelineStorage, host: str = MOCK_HOST, port: int = MOCK_PORT):
        self.storage = storage
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.sock_file = None
        
        # State per sensor
        self.current_session_id: Optional[str] = None
        self.next_expected_index: Dict[int, int] = {1: 0, 2: 0}
        self.is_running = True

    def connect(self) -> bool:
        """Connect or reconnect to mock service."""
        self.close()
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5.0)
            s.connect((self.host, self.port))
            self.sock = s
            self.sock_file = s.makefile("rwb")
            logger.info(f"Connected to mock service at {self.host}:{self.port}")
            return True
        except Exception as e:
            self.close()
            return False

    def close(self):
        if self.sock_file:
            try:
                self.sock_file.close()
            except Exception:
                pass
            self.sock_file = None
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def _extract_data_substring_and_crc(self, raw_bytes: bytes) -> Tuple[bytes, str]:
        """Extract the exact `data` JSON array substring from [ to matching ]
        and compute CRC-32 as specified in Section 5/6.
        """
        # Find `"data":` or `'data':`
        key_pos = raw_bytes.find(b'"data":')
        if key_pos == -1:
            key_pos = raw_bytes.find(b"'data':")
        
        if key_pos == -1:
            return b"[]", ""
        
        # Find start bracket
        start = raw_bytes.find(b"[", key_pos)
        if start == -1:
            return b"[]", ""
        
        # Find matching outer closing bracket
        # Since JSON array ends before the closing '}', find the last ']'
        end = raw_bytes.rfind(b"]")
        if end == -1 or end <= start:
            return b"[]", ""
        
        data_text = raw_bytes[start : end + 1]
        calc_crc = hex(zlib.crc32(data_text) & 0xFFFFFFFF)[2:].zfill(8).lower()
        return data_text, calc_crc

    def read_sensor(self, sensor: int, retry_from_index: Optional[int] = None) -> bool:
        """Request next batch of data for sensor, check CRC, and save to storage."""
        if not self.sock_file:
            if not self.connect():
                return False

        req = {"cmd": "read", "sensor": sensor}
        if retry_from_index is not None:
            req["from_index"] = retry_from_index

        try:
            req_line = json.dumps(req).encode("utf-8") + b"\n"
            self.sock_file.write(req_line)
            self.sock_file.flush()

            raw_line = self.sock_file.readline()
            if not raw_line:
                self.close()
                return False

            reply = json.loads(raw_line.decode("utf-8"))
            session_id = reply.get("session_id")
            first_index = reply.get("first_index", 0)
            count = reply.get("count", 0)
            lost_ranges = reply.get("lost", [])
            crc_received = (reply.get("crc32") or "").lower()
            data = reply.get("data", [])

            # Handle Mock Restart / Session Change
            if self.current_session_id is not None and session_id != self.current_session_id:
                logger.warning(f"Mock session changed: {self.current_session_id} -> {session_id}")
                # Record loss for unread remainder of old session
                for s in (1, 2):
                    self.storage.record_loss(
                        session_id=self.current_session_id,
                        sensor=s,
                        from_index=self.next_expected_index[s],
                        to_index=None,
                        reason="mock_restart_session_lost"
                    )
                self.next_expected_index = {1: 0, 2: 0}

            self.current_session_id = session_id
            self.storage.set_meta("active_session_id", session_id)

            # Record any losses reported by mock
            for l in lost_ranges:
                self.storage.record_loss(
                    session_id=session_id,
                    sensor=sensor,
                    from_index=l["from_index"],
                    to_index=l["to_index"],
                    reason=l.get("reason", "mock_reported_loss")
                )
                # Advance expected index if loss was beyond expected
                if l["to_index"] >= self.next_expected_index[sensor]:
                    self.next_expected_index[sensor] = l["to_index"] + 1

            # CRC-32 Validation
            _, calc_crc = self._extract_data_substring_and_crc(raw_line)
            if calc_crc != crc_received:
                logger.error(f"CRC mismatch for sensor {sensor} at index {first_index}: got {crc_received}, calculated {calc_crc}. Rejecting corrupt payload.")
                # Re-request from the expected index
                time.sleep(0.05)
                return self.read_sensor(sensor, retry_from_index=first_index)

            # Valid Data Received -> Save to Disk immediately
            if count > 0 and data:
                self.storage.insert_samples(
                    session_id=session_id,
                    sensor=sensor,
                    first_index=first_index,
                    data=data
                )
                self.next_expected_index[sensor] = first_index + count
            
            return True

        except (socket.timeout, ConnectionResetError, BrokenPipeError, json.JSONDecodeError) as e:
            logger.warning(f"Socket read exception on sensor {sensor}: {e}")
            self.close()
            return False
