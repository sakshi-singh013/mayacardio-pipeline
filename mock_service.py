"""Mock sensor service imitating biosignal-mock (Sections 6, 6.1, 6.2, 6.3).

Generates synthetic ECG/PPG signals or deterministic ramps and serves them
over TCP newline-delimited JSON. Used for local testing and validation.
"""
import socket
import json
import zlib
import time
import math
import argparse
import uuid
from typing import Dict, List, Any, Optional


class BiosignalMock:
    def __init__(self, mode: str = "signal", seed: int = 7, duration: int = 3600, port: int = 7100):
        self.mode = mode
        self.seed = seed
        self.duration = duration
        self.port = port
        self.sample_rate = 400
        self.buffer_size = 4000  # 10 seconds buffer
        self.session_id = uuid.uuid4().hex[:8]
        
        # State tracking
        self.start_time = time.time()
        self.last_read_index = {1: 0, 2: 0}
        self.is_running = True

    def _sample_sensor_1(self, i: int) -> List[int]:
        """Generate sensor 1 (3 channels)."""
        if self.mode == "ramp":
            return [
                (3 * i + 0) % 8388608,
                (3 * i + 1) % 8388608,
                (3 * i + 2) % 8388608
            ]
        else:
            # Signal mode: ECG-like signal
            t_sec = i / float(self.sample_rate)
            minute = (t_sec / 60.0) % 6.0
            if minute < 2.0:
                bpm = 60.0
            elif minute < 4.0:
                bpm = 90.0
            else:
                bpm = 72.0
            
            period_samples = int((60.0 / bpm) * self.sample_rate)
            beat_pos = i % period_samples
            
            # Baseline wander + QRS complex peak
            baseline = 50000 + int(3000 * math.sin(2 * math.pi * 0.15 * t_sec))
            if 0 <= beat_pos <= 20:
                # Sharp QRS peak
                peak = int(600000 * math.sin(math.pi * (beat_pos / 20.0)))
            else:
                peak = int(20000 * math.sin(2 * math.pi * beat_pos / period_samples))
            
            ch2 = baseline + peak
            ch1 = int(ch2 * 0.6)
            ch3 = int(ch2 * 1.3)
            return [ch1, ch2, ch3]

    def _sample_sensor_2(self, i: int) -> List[int]:
        """Generate sensor 2 (2 channels)."""
        if self.mode == "ramp":
            return [
                (2 * i + 0) % 8388608,
                (2 * i + 1) % 8388608
            ]
        else:
            t_sec = i / float(self.sample_rate)
            ch1 = 400000 + int(15000 * math.sin(2 * math.pi * 0.05 * t_sec))
            ch2 = 150000 + int(8000 * math.cos(2 * math.pi * 0.08 * t_sec))
            return [ch1, ch2]

    def handle_read(self, req: Dict[str, Any], inject_fault: Optional[str] = None) -> bytes:
        sensor = req.get("sensor", 1)
        now_sec = time.time() - self.start_time
        current_max_sample = min(int(now_sec * self.sample_rate), self.duration * self.sample_rate)
        
        from_index = req.get("from_index")
        if from_index is None:
            from_index = self.last_read_index.get(sensor, 0)
        
        oldest_allowed = max(0, current_max_sample - self.buffer_size)
        lost = []
        if from_index < oldest_allowed:
            lost.append({
                "from_index": from_index,
                "to_index": oldest_allowed - 1,
                "reason": "buffer_overflow"
            })
            from_index = oldest_allowed

        # Read at most 1 second of data (400 samples)
        count = min(400, max(0, current_max_sample - from_index))
        data = []
        for idx in range(from_index, from_index + count):
            if sensor == 1:
                data.append(self._sample_sensor_1(idx))
            else:
                data.append(self._sample_sensor_2(idx))

        self.last_read_index[sensor] = from_index + count
        session_end = (from_index + count >= self.duration * self.sample_rate)

        data_json = json.dumps(data)
        # CRC32 of data text exactly as sent: from [ to matching ]
        calc_crc32 = hex(zlib.crc32(data_json.encode("utf-8")) & 0xFFFFFFFF)[2:].zfill(8)

        if inject_fault == "corrupt_crc32":
            calc_crc32 = "deadbeef"

        reply_obj = {
            "session_id": self.session_id,
            "sensor": sensor,
            "first_index": from_index,
            "count": count,
            "lost": lost,
            "session_end": session_end,
            "crc32": calc_crc32,
            "data": data
        }
        return json.dumps(reply_obj).encode("utf-8") + b"\n"

    def run(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", self.port))
        server.listen(1)
        print(f"[biosignal-mock] Listening on port {self.port} (mode: {self.mode}, session: {self.session_id})")

        while self.is_running:
            try:
                conn, addr = server.accept()
                with conn:
                    f = conn.makefile("rwb")
                    while True:
                        line = f.readline()
                        if not line:
                            break
                        try:
                            req = json.loads(line.decode("utf-8"))
                            if req.get("cmd") == "read":
                                resp = self.handle_read(req)
                                f.write(resp)
                                f.flush()
                        except Exception as e:
                            print(f"[biosignal-mock] Error handling request: {e}")
                            break
            except Exception as e:
                if self.is_running:
                    print(f"[biosignal-mock] Server accept loop error: {e}")
                    time.sleep(0.5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Python Biosignal Mock Service")
    parser.add_argument("--mode", default="signal", choices=["signal", "ramp"])
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--duration", type=int, default=3600)
    parser.add_argument("--port", type=int, default=7100)
    args = parser.parse_args()

    mock = BiosignalMock(mode=args.mode, seed=args.seed, duration=args.duration, port=args.port)
    mock.run()
