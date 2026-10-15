"""Cloud Receiver and Processor Service.

Can run on any free cloud instance or locally as the cloud tier.
Accepts live streamed batches and catch-up uploads, persists them to cloud storage,
and executes Section 3 processing logic producing online results within 5 seconds.
"""
import json
import time
import argparse
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs
from typing import Dict, Any

from config import CLOUD_HOST, CLOUD_PORT, CLOUD_DB_PATH
from storage import PipelineStorage
from processor import process_window


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class CloudServerHandler(BaseHTTPRequestHandler):
    storage: PipelineStorage = None
    last_processed_window: Dict[str, int] = {}

    def log_message(self, format, *args):
        # Suppress noisy standard HTTP access logs
        pass

    def _send_json(self, status_code: int, data: Any):
        payload = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        return json.loads(body.decode("utf-8"))

    def do_GET(self):
        url = urlparse(self.path)
        path = url.path
        params = parse_qs(url.query)

        if path in ("/health", "/api/ping"):
            self._send_json(200, {"status": "ok", "time": time.time()})
            return

        if path == "/api/status":
            session_id = params.get("session", [None])[0]
            counts = self.storage.get_status_counts(session_id)
            self._send_json(200, counts)
            return

        if path == "/api/export":
            session_id = params.get("session", [None])[0]
            if not session_id:
                self._send_json(400, {"error": "Missing session query parameter"})
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.end_headers()
            for line in self.storage.export_samples_and_losses(session_id):
                self.wfile.write((line + "\n").encode("utf-8"))
            return

        if path == "/api/results":
            session_id = params.get("session", [None])[0]
            if not session_id:
                self._send_json(400, {"error": "Missing session query parameter"})
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.end_headers()
            for line in self.storage.export_results(session_id):
                self.wfile.write((line + "\n").encode("utf-8"))
            return

        self._send_json(404, {"error": "Not Found"})

    def do_POST(self):
        url = urlparse(self.path)
        path = url.path

        if path in ("/api/stream", "/api/sync"):
            try:
                body = self._read_json()
                samples = body.get("samples", [])
                losses = body.get("losses", [])
                
                # Ingest samples
                grouped: Dict[tuple, list] = {}
                for s in samples:
                    key = (s["session_id"], s["sensor"])
                    grouped.setdefault(key, []).append((s["index"], s["values"]))

                for (sess, sensor), items in grouped.items():
                    items.sort(key=lambda x: x[0])
                    # Insert in continuous blocks
                    curr_batch = []
                    curr_first = None
                    expected_next = None
                    for idx, vals in items:
                        if curr_first is None:
                            curr_first = idx
                            curr_batch.append(vals)
                            expected_next = idx + 1
                        elif idx == expected_next:
                            curr_batch.append(vals)
                            expected_next += 1
                        else:
                            self.storage.insert_samples(sess, sensor, curr_first, curr_batch, synced=True)
                            curr_first = idx
                            curr_batch = [vals]
                            expected_next = idx + 1
                    if curr_batch and curr_first is not None:
                        self.storage.insert_samples(sess, sensor, curr_first, curr_batch, synced=True)

                # Ingest losses
                for l in losses:
                    self.storage.record_loss(
                        session_id=l["session_id"],
                        sensor=l["sensor"],
                        from_index=l["from_index"],
                        to_index=l.get("to_index"),
                        reason=l["reason"]
                    )

                self._send_json(200, {"status": "success", "ingested": len(samples)})
            except Exception as e:
                self._send_json(500, {"error": str(e)})
            return

        self._send_json(404, {"error": "Not Found"})


def process_session_windows(storage: PipelineStorage, session_id: str, mode: str = "online"):
    """Compute and save Section 3 window processing results for all newly completed windows."""
    with storage._get_connection() as conn:
        row_done = conn.execute(
            "SELECT MAX(window) as max_done FROM results WHERE session_id = ? AND mode = ?;",
            (session_id, mode)
        ).fetchone()
        last_w = row_done["max_done"] if (row_done and row_done["max_done"] is not None) else -1

        row_samp = conn.execute(
            "SELECT MAX(sample_index) as max_idx FROM samples WHERE session_id = ?;",
            (session_id,)
        ).fetchone()
        if not row_samp or row_samp["max_idx"] is None:
            return
        max_w = row_samp["max_idx"] // 400

    for w in range(last_w + 1, max_w):
        start_i = 400 * w
        end_i = 400 * w + 399
        s1_data = storage.get_window_sensor_data(session_id, 1, start_i, end_i)
        s2_data = storage.get_window_sensor_data(session_id, 2, start_i, end_i)
        hr_start = max(0, 400 * (w - 6))
        hr_end = 400 * (w + 1)
        s1_ch2 = storage.get_sensor1_ch2_slice(session_id, hr_start, hr_end)

        res = process_window(session_id, w, mode, s1_data, s2_data, s1_ch2)
        storage.save_window_result(res)


# Expose on CloudServerHandler for backwards compatibility
CloudServerHandler._check_and_process_windows = lambda self, session_id: process_session_windows(CloudServerHandler.storage, session_id, "online")


def _cloud_background_processor(storage: PipelineStorage, stop_event: threading.Event):
    """Background worker continuously computing Section 3 windows on the cloud."""
    while not stop_event.is_set():
        try:
            with storage._get_connection() as conn:
                rows = conn.execute("SELECT DISTINCT session_id FROM samples;").fetchall()
                sessions = [r["session_id"] for r in rows]

            for sess in sessions:
                process_session_windows(storage, sess, mode="online")
        except Exception:
            pass
        time.sleep(0.5)


def run_cloud_server(host: str = CLOUD_HOST, port: int = CLOUD_PORT, db_path: str = CLOUD_DB_PATH):
    storage = PipelineStorage(db_path)
    CloudServerHandler.storage = storage
    server = ThreadedHTTPServer((host, port), CloudServerHandler)
    
    stop_event = threading.Event()
    proc_thread = threading.Thread(target=_cloud_background_processor, args=(storage, stop_event), daemon=True)
    proc_thread.start()

    print(f"[CloudServer] Running on http://{host}:{port} (db: {db_path})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("[CloudServer] Shutting down...")
    finally:
        stop_event.set()
        server.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cloud Receiver & Processor Service")
    parser.add_argument("--host", default=CLOUD_HOST)
    parser.add_argument("--port", type=int, default=CLOUD_PORT)
    parser.add_argument("--db", default=CLOUD_DB_PATH)
    args = parser.parse_args()

    run_cloud_server(host=args.host, port=args.port, db_path=args.db)
