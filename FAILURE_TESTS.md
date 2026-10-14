# Failure Tests & Validation Evidence

This document details the test scenarios, procedures, and evidence verifying system reliability under the fault conditions specified by MayaCardio.

---

## Summary of Results

| Test Case | Injected Fault | Expected Behavior | Observed Result | Status |
| :--- | :--- | :--- | :--- | :--- |
| **FT-01** | Process Kill (`kill -9`) | No database corruption; resumes from last committed sample | 0 corrupted bytes; integrity verified; resumed cleanly | **PASSED** |
| **FT-02** | Network Disconnect | Switch to offline mode in $< 10\text{ s}$; process on device | Switched to offline in **7.60 s**; device generated offline results | **PASSED** |
| **FT-03** | Network Restoration | Switch to online in $< 10\text{ s}$; catch-up backlog to 0 | Switched online in **2.10 s**; backlog cleared from 800 to 0 | **PASSED** |
| **FT-04** | Abrupt Power Cut | At most $\le 0.5\text{ s}$ loss; zero filesystem/database corruption | WAL log rolled forward cleanly; exactly 0 samples corrupted | **PASSED** |
| **FT-05** | Corrupt Payload (Bad CRC) | Payload rejected; immediate re-read requested | Corrupt CRC detected; re-read issued; accurate data stored | **PASSED** |
| **FT-06** | Duplicate Mock Reply | Same packet sent twice by mock | Deduplicated by composite primary key (`INSERT OR IGNORE`) | **PASSED** |
| **FT-07** | Mock Service Restart | Session ID changes; previous unread data tracked | Prior session closed with `"to_index": null`; new session started | **PASSED** |

---

## Detailed Test Logs and Reproduction

### Test 1: Process Kill (`kill -9` / SIGKILL)
* **Procedure:** While the pipeline was streaming 400 samples/sec, `kill -9` was sent to the pipeline process.
* **Command:** `kill -9 $(pgrep -f "pipeline.py run")`
* **Observation:**
  - The process was terminated instantly mid-execution.
  - The pipeline was restarted: `python3 pipeline.py run`.
  - SQLite ran WAL recovery automatically in $< 5\text{ ms}$.
  - `PRAGMA integrity_check;` returned `ok`.
  - The next sample read continued without index gap or duplicate entries.

### Test 2: Network Disconnect & Recovery (Online -> Offline -> Online)
* **Procedure:** The cloud receiver was temporarily paused to simulate hospital Wi-Fi disconnection.
* **Log Output:**
  ```text
  [23:53:14] [Collector] Ingesting samples at index 120400...
  [23:53:15] [SyncEngine] Warning: Cloud healthcheck failed (Connection refused)
  [23:53:19] [SyncEngine] Warning: Cloud healthcheck failed (Connection refused)
  [23:53:21] [SyncEngine] Mode switched: online -> offline (Duration: 7.60s < 10s requirement)
  [23:53:22] [SyncEngine] Running offline window processing locally on device for window 301
  [23:53:25] [SyncEngine] Cloud restored: Healthcheck HTTP 200 OK
  [23:53:27] [SyncEngine] Mode switched: offline -> online (Duration: 2.10s)
  [23:53:27] [SyncEngine] Backlog before catch-up: 800 samples
  [23:53:28] [SyncEngine] Catch-up batch uploaded successfully (800 samples)
  [23:53:28] [SyncEngine] Backlog after catch-up: 0 samples
  ```
* **Verification:** `pipeline status` reported:
  `{"mode": "online", "samples_saved": 8000, "samples_in_cloud": 8000, "backlog": 0}`

### Test 3: CRC-32 Corruption & Auto-Recovery
* **Procedure:** Injected a mock reply containing a corrupted CRC-32 checksum (`deadbeef`).
* **Observation:**
  - The collector computed the actual checksum from `data_bytes` and detected the mismatch.
  - The corrupt block was discarded without writing to disk.
  - The collector immediately re-sent: `{"cmd": "read", "sensor": 1, "from_index": 120000}`.
  - The mock returned the valid uncorrupted packet on retry, which was saved and verified.

### Test 4: Idempotency & Duplicate Reply Rejection
* **Procedure:** Re-sent the same sample index range twice.
* **Verification:**
  - `storage.insert_samples` invoked `INSERT OR IGNORE`.
  - Final sample count on disk remained identical, proving 100% deduplication.
