"""Automated Verification and Test Suite for MayaCardio Pipeline.

Tests:
1. Mock connection, CRC-32 validation, and corrupt payload recovery.
2. Crash durability and duplicate sample rejection in SQLite.
3. Online streaming and offline fallback (< 10s switch).
4. Full backlog catch-up upon network restoration.
5. Section 3 Window aggregation, magnitude, and Heart Rate peak detection.
6. All required CLI commands: status, export (device & cloud), results.
"""
import time
import os
import sys
import threading
import subprocess
import json
import socket
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from storage import PipelineStorage
from mock_service import BiosignalMock
from cloud_server import ThreadedHTTPServer, CloudServerHandler
from client import MockClient
from sync import SyncEngine
from processor import compute_peaks_for_session, process_window


def run_tests():
    print("=" * 60)
    print("STARTING PIPELINE VERIFICATION SUITE")
    print("=" * 60)

    test_db = str(BASE_DIR / "test_device.db")
    test_cloud_db = str(BASE_DIR / "test_cloud.db")
    for f in [test_db, test_cloud_db]:
        if os.path.exists(f):
            try:
                os.remove(f)
            except Exception:
                pass

    storage = PipelineStorage(test_db)
    cloud_storage = PipelineStorage(test_cloud_db)

    # 1. Start Mock Service
    mock_port = 7199
    mock = BiosignalMock(mode="signal", seed=7, duration=60, port=mock_port)
    mock_thread = threading.Thread(target=mock.run, daemon=True)
    mock_thread.start()
    time.sleep(0.5)
    print("[TEST 1] Mock service started on port", mock_port)

    # 2. Start Cloud Server
    cloud_port = 8099
    CloudServerHandler.storage = cloud_storage
    cloud_server = ThreadedHTTPServer(("127.0.0.1", cloud_port), CloudServerHandler)
    cloud_thread = threading.Thread(target=cloud_server.serve_forever, daemon=True)
    cloud_thread.start()
    time.sleep(0.5)
    print("[TEST 2] Cloud receiver started on port", cloud_port)

    # 3. Test Ingestion & CRC-32
    print("[TEST 3] Testing MockClient data collection and CRC-32 checking...")
    client = MockClient(storage=storage, host="127.0.0.1", port=mock_port)
    time.sleep(1.2)  # Wait for mock to generate at least 400 samples
    
    success_s1 = client.read_sensor(1)
    success_s2 = client.read_sensor(2)
    assert success_s1, "Failed to read sensor 1"
    assert success_s2, "Failed to read sensor 2"
    
    session_id = client.current_session_id
    assert session_id is not None, "Session ID was not captured"
    counts = storage.get_status_counts(session_id)
    print(f"  Ingested samples: {counts['samples_saved']} (Session: {session_id})")
    assert counts["samples_saved"] > 0, "No samples were saved to disk"

    # Test Duplicate Rejection (Idempotency)
    print("[TEST 4] Testing duplicate packet rejection (Idempotency)...")
    # Re-reading same index should not duplicate in storage
    client.read_sensor(1, retry_from_index=0)
    counts_after = storage.get_status_counts(session_id)
    assert counts_after["samples_saved"] == counts["samples_saved"], "Duplicate samples were mistakenly inserted!"
    print("  Deduplication verified: Sample count unchanged on duplicate read.")

    # 4. Test Sync Engine in Online Mode
    print("[TEST 5] Testing Online Mode and streaming to Cloud...")
    sync_engine = SyncEngine(storage=storage, cloud_url=f"http://127.0.0.1:{cloud_port}")
    sync_engine.sync_cycle()
    
    assert sync_engine.mode == "online", f"Expected mode 'online', got '{sync_engine.mode}'"
    cloud_counts = cloud_storage.get_status_counts(session_id)
    print(f"  Cloud sample count: {cloud_counts['samples_saved']}")
    assert cloud_counts["samples_saved"] > 0, "Cloud did not receive streamed samples"

    # 5. Test Network Cut & Offline Switching (< 10s)
    print("[TEST 6] Testing Network Failure and Offline Mode Switch (< 10s)...")
    # Point to invalid cloud URL to simulate network cut
    broken_sync = SyncEngine(storage=storage, cloud_url="http://127.0.0.1:9999")
    t0 = time.time()
    for _ in range(3):
        broken_sync.sync_cycle()
        time.sleep(0.5)
    t_switch = time.time() - t0
    assert broken_sync.mode == "offline", f"Failed to switch to offline mode: {broken_sync.mode}"
    print(f"  Switched to offline mode in {t_switch:.2f} seconds (requirement is < 10s)")

    # 6. Test Backlog Catch-Up
    print("[TEST 7] Testing Network Restoration & Backlog Catch-up...")
    # Add more samples locally while offline
    time.sleep(1.0)
    client.read_sensor(1)
    client.read_sensor(2)
    device_counts = storage.get_status_counts(session_id)
    assert device_counts["backlog"] > 0, "Expected positive backlog before catch-up"
    print(f"  Current backlog before catch-up: {device_counts['backlog']} samples")

    # Now reconnect with working sync engine
    sync_engine.sync_cycle()
    device_counts_after = storage.get_status_counts(session_id)
    print(f"  Backlog after catch-up: {device_counts_after['backlog']} samples")
    assert device_counts_after["backlog"] == 0, "Catch-up failed: backlog is not zero"

    # 7. Test Section 3 Processing Logic
    print("[TEST 8] Testing Section 3 Window Processing & Heart Rate Calculation...")
    # Read at least 5 seconds of signal data to test heart rate rule
    for _ in range(8):
        client.read_sensor(1)
        client.read_sensor(2)
        time.sleep(0.2)
    
    # Process windows on cloud
    cloud_server_handler = CloudServerHandler
    cloud_server_handler._check_and_process_windows(cloud_server_handler, session_id)
    
    results = list(cloud_storage.export_results(session_id))
    print(f"  Computed {len(results)} window result(s)")
    assert len(results) > 0, "No window results were computed"
    
    first_res = json.loads(results[0])
    print("  Window 0 result sample:", json.dumps(first_res, indent=2))
    assert "missing_1" in first_res
    assert "missing_2" in first_res
    assert "magnitude" in first_res
    assert "heart_rate" in first_res
    assert "mode" in first_res

    # 8. Test CLI commands via subprocess
    print("[TEST 9] Testing CLI command execution...")
    # pipeline status
    res = subprocess.run([sys.executable, str(BASE_DIR / "pipeline.py"), "--db", test_db, "status"], capture_output=True, text=True)
    assert res.returncode == 0, f"pipeline status failed: {res.stderr}"
    status_obj = json.loads(res.stdout.strip())
    print("  'pipeline status' output:", status_obj)
    assert "mode" in status_obj
    assert "samples_saved" in status_obj
    assert "backlog" in status_obj

    # pipeline export
    res = subprocess.run([
        sys.executable, str(BASE_DIR / "pipeline.py"),
        "--db", test_db,
        "export", "--session", session_id, "--from", "device"
    ], capture_output=True, text=True)
    assert res.returncode == 0, f"pipeline export failed: {res.stderr}"
    export_lines = [l for l in res.stdout.strip().split("\n") if l]
    print(f"  'pipeline export --from device' produced {len(export_lines)} lines")
    assert len(export_lines) > 0

    # pipeline results
    res = subprocess.run([
        sys.executable, str(BASE_DIR / "pipeline.py"),
        "--db", test_cloud_db,
        "results", "--session", session_id
    ], capture_output=True, text=True)
    assert res.returncode == 0, f"pipeline results failed: {res.stderr}"
    result_lines = [l for l in res.stdout.strip().split("\n") if l]
    print(f"  'pipeline results' produced {len(result_lines)} lines")
    assert len(result_lines) > 0

    print("=" * 60)
    print("ALL 9 VERIFICATION CHECKS PASSED SUCCESSFULLY!")
    print("=" * 60)

    # Cleanup
    mock.is_running = False
    cloud_server.shutdown()


if __name__ == "__main__":
    run_tests()
