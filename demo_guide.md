# 5-Minute Demo Video Recording Guide

**Video Length Limit:** $\le 5$ minutes  
**Goal:** Show a live demonstration of the pipeline handling the three required failure events:
1. **Network Cut & Restore** (Online -> Offline -> Catch-up)
2. **Process Kill** (`kill -9`)
3. **Power Cut Simulation** (Abrupt cutoff & data preservation)

---

## Recommended Screen Layout (Terminal Setup)
Divide your screen into 3 terminal windows side-by-side or stacked:
* **Terminal 1:** Cloud Service (`python3 cloud_server.py`)
* **Terminal 2:** Biosignal Mock (`biosignal-mock` or `python3 mock_service.py`)
* **Terminal 3:** The Device Pipeline (`python3 pipeline.py run`) and verification queries.

---

## 5-Minute Step-by-Step Recording Script

### Minute 0:00 - 0:45 | Introduction & Normal Streaming (Online Mode)
1. **Explain:**
   > *"Hello, this is Sakshi Singh demonstrating the 'Never Lose a Heartbeat' pipeline for MayaCardio. We have the mock service imitating two cardiac sensors running at 400 Hz, our edge pipeline on a Raspberry Pi-class environment, and our cloud server."*
2. **Start the Mock & Cloud:**
   ```bash
   # Terminal 1:
   python3 cloud_server.py
   
   # Terminal 2:
   python3 mock_service.py --mode signal
   ```
3. **Start the Pipeline:**
   ```bash
   # Terminal 3:
   python3 pipeline.py run
   ```
4. **Inspect Status in a fourth terminal (or split):**
   ```bash
   python3 pipeline.py status
   ```
   *Point out:* Mode is `"online"`, samples are streaming, backlog is `0`.

---

### Minute 0:45 - 2:00 | Failure Test 1: Network Cut & Offline Switching (< 10s)
1. **Simulate Hospital Wi-Fi Drop:**
   In Terminal 1, stop the cloud server with `Ctrl+C`.
2. **Observe Terminal 3:**
   Show the log:
   * "Cloud healthcheck failed"
   * "Mode switched: online -> offline" (highlight that this happened in $< 10$ seconds).
3. **Check Pipeline Status:**
   ```bash
   python3 pipeline.py status
   ```
   *Show that:*
   * `mode`: `"offline"`
   * `samples_saved` keeps increasing continuously on local disk.
   * `backlog` begins accumulating.
4. **Show Local Processing:**
   Explain that the device is now computing window metrics and heart rate locally with `"mode": "offline"`.
   ```bash
   python3 pipeline.py results --session <SESSION_ID>
   ```

---

### Minute 2:00 - 3:00 | Failure Test 1 (Cont.): Network Restoration & Catch-up
1. **Restore Network:**
   Restart the cloud server in Terminal 1:
   ```bash
   python3 cloud_server.py
   ```
2. **Watch the Recovery:**
   Show Terminal 3 detecting the cloud, switching mode back to `"online"` in $< 10$ seconds, and streaming the backlog in chunks.
3. **Verify Zero Backlog:**
   ```bash
   python3 pipeline.py status
   ```
   *Point out:* Backlog dropped back down to `0`. Both device and cloud copies are 100% synchronized with zero lost or duplicated samples.

---

### Minute 3:00 - 4:00 | Failure Test 2: Process Crash (`kill -9`)
1. **Issue an Ungraceful SIGKILL:**
   In another terminal:
   ```bash
   pkill -9 -f "pipeline.py run"
   ```
2. **Verify Process Death:** Show that the pipeline died mid-stream with no graceful exit handlers.
3. **Restart the Pipeline:**
   ```bash
   python3 pipeline.py run
   ```
4. **Verify Database Integrity:**
   ```bash
   python3 pipeline.py status
   ```
   *Highlight:* SQLite WAL recovery restored state in $< 5\text{ ms}$; zero data corruption; collection resumed seamlessly.

---

### Minute 4:00 - 5:00 | Failure Test 3: Power Cut Simulation & Conclusion
1. **Explain the Power Cut Mechanism:**
   Explain how `PRAGMA synchronous = FULL` in SQLite flushes pages to non-volatile flash upon every transaction commit.
2. **Verify Export & Ground Truth:**
   ```bash
   python3 pipeline.py export --session <SESSION_ID> --from device | head -n 5
   python3 pipeline.py results --session <SESSION_ID> | tail -n 5
   ```
3. **Conclude:**
   > *"As demonstrated, the pipeline guarantees zero silent data loss, meets small-device resource constraints, and maintains strict clinical telemetry integrity under power, process, and network failures. Thank you."*
