#!/usr/bin/env python3
"""Unified CLI executable for the MayaCardio data pipeline.

Implements all required commands:
- pipeline status
- pipeline export --session ID --from [device|cloud]
- pipeline results --session ID
- pipeline run (starts collector, sync engine, and local worker)
"""
import sys
import os
import json
import time
import argparse
import threading
import urllib.request
import urllib.parse
from typing import Optional

from config import (
    DEVICE_DB_PATH,
    CLOUD_URL,
    MOCK_HOST,
    MOCK_PORT
)
from storage import PipelineStorage
from client import MockClient
from sync import SyncEngine


def cmd_status(storage: PipelineStorage, cloud_url: str = CLOUD_URL):
    """Print current mode, samples saved, samples in the cloud, and backlog as one JSON line."""
    mode = "offline"
    try:
        req = urllib.request.Request(f"{cloud_url.rstrip('/')}/health", method="GET")
        with urllib.request.urlopen(req, timeout=0.5) as resp:
            if resp.status == 200:
                mode = "online"
    except Exception:
        mode = "offline"

    storage.set_meta("current_mode", mode)
    session_id = storage.get_meta("active_session_id")
    counts = storage.get_status_counts(session_id)
    
    status_line = json.dumps({
        "mode": mode,
        "samples_saved": counts["samples_saved"],
        "samples_in_cloud": counts["samples_in_cloud"],
        "backlog": counts["backlog"]
    })
    print(status_line)


def cmd_export(storage: PipelineStorage, session_id: str, from_source: str, cloud_url: str):
    """Export samples and loss records as JSON lines."""
    if from_source == "device":
        for line in storage.export_samples_and_losses(session_id):
            print(line)
    elif from_source == "cloud":
        url = f"{cloud_url.rstrip('/')}/api/export?session={urllib.parse.quote(session_id)}"
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                for line in resp:
                    decoded = line.decode("utf-8").strip()
                    if decoded:
                        print(decoded)
        except Exception as e:
            sys.stderr.write(f"Error fetching export from cloud: {e}\n")
            sys.exit(1)
    else:
        sys.stderr.write(f"Unknown source: {from_source}. Must be 'device' or 'cloud'.\n")
        sys.exit(1)


def cmd_results(storage: PipelineStorage, session_id: str):
    """Export window processing outputs as JSON lines."""
    for line in storage.export_results(session_id):
        print(line)


def run_pipeline_daemon(storage: PipelineStorage, mock_host: str, mock_port: int, cloud_url: str):
    """Run data ingestion and synchronization loops concurrently."""
    client = MockClient(storage=storage, host=mock_host, port=mock_port)
    sync_engine = SyncEngine(storage=storage, cloud_url=cloud_url)

    stop_event = threading.Event()

    def collector_worker():
        while not stop_event.is_set():
            # Alternate reading sensor 1 and sensor 2
            got_s1 = client.read_sensor(1)
            got_s2 = client.read_sensor(2)
            if not (got_s1 or got_s2):
                time.sleep(0.05)

    def sync_worker():
        while not stop_event.is_set():
            try:
                sync_engine.sync_cycle()
            except Exception as e:
                pass
            time.sleep(0.1)

    t_collector = threading.Thread(target=collector_worker, daemon=True)
    t_sync = threading.Thread(target=sync_worker, daemon=True)

    t_collector.start()
    t_sync.start()

    print("[Pipeline] Running pipeline daemon (Collector + Sync Engine)... Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("[Pipeline] Stopping pipeline daemon...")
        stop_event.set()
        t_collector.join(timeout=1.0)
        t_sync.join(timeout=1.0)
        client.close()


def main():
    parser = argparse.ArgumentParser(prog="pipeline", description="MayaCardio Resilient Biosignal Pipeline")
    parser.add_argument("--db", default=DEVICE_DB_PATH, help="Path to device SQLite database")
    parser.add_argument("--cloud-url", default=CLOUD_URL, help="URL of cloud receiver service")
    parser.add_argument("--mock-host", default=MOCK_HOST, help="Mock service host")
    parser.add_argument("--mock-port", type=int, default=MOCK_PORT, help="Mock service port")

    subparsers = parser.add_subparsers(dest="command")

    # 1. status
    subparsers.add_parser("status", help="Print status as one JSON line")

    # 2. export
    export_parser = subparsers.add_parser("export", help="Export session data and losses")
    export_parser.add_argument("--session", required=True, help="Session ID to export")
    export_parser.add_argument("--from", dest="from_source", required=True, choices=["device", "cloud"], help="Source to export from")

    # 3. results
    results_parser = subparsers.add_parser("results", help="Export window results")
    results_parser.add_argument("--session", required=True, help="Session ID to export")

    # 4. run
    subparsers.add_parser("run", help="Run the continuous edge pipeline daemon")

    args = parser.parse_args()

    storage = PipelineStorage(args.db)

    if args.command == "status":
        cmd_status(storage, cloud_url=args.cloud_url)
    elif args.command == "export":
        cmd_export(storage, session_id=args.session, from_source=args.from_source, cloud_url=args.cloud_url)
    elif args.command == "results":
        cmd_results(storage, session_id=args.session)
    elif args.command == "run":
        run_pipeline_daemon(storage, mock_host=args.mock_host, mock_port=args.mock_port, cloud_url=args.cloud_url)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
