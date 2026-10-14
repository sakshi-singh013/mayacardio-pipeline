# MayaCardio — Never Lose a Heartbeat

A resilient, edge-to-cloud biosignal data pipeline engineered to run on an **ARM64 Raspberry Pi-class board** ($\le 4\text{ CPU cores}$, $\le 2\text{ GB RAM}$) and a **free cloud service**.

It connects to continuous cardiac sensors, ensures **zero silent data loss**, switches dynamically between **online streaming** and **offline edge processing** upon network failure ($< 10\text{ s}$), and survives process crashes (`kill -9`), power pulls, and network dropouts.

---

## 1. Key Features

* **Strict Persistence Guarantee:** Every sample received is written immediately to an ACID-compliant SQLite WAL database on disk with `PRAGMA synchronous = FULL`.
* **Sub-10s Auto-Switching:** Detects network availability and switches between online cloud streaming and offline local processing within 10 seconds.
* **Catch-up Convergence:** Outages up to 2 hours are uploaded upon reconnection, reaching zero backlog within minutes without data loss or duplication.
* **Corrupt Payload Defense:** Full CRC-32 wire-level checksum verification on all TCP responses. Re-reads corrupt payloads automatically.
* **Clinical Telemetry Processing (Section 3):** Standardized 1-second window aggregation, channel metrics, magnitude, and 5-second sliding-window Heart Rate peak calculation.
* **Standard Compliant CLI:** One unified binary `pipeline` implementing `pipeline status`, `pipeline export`, and `pipeline results`.

---

## 2. Repository Structure

```
mayacardio-pipeline/
├── config.py              # Configuration constants & network settings
├── storage.py             # SQLite WAL crash-proof persistence engine
├── processor.py           # Window aggregation & Heart Rate peak detection
├── client.py              # TCP Mock Client with CRC-32 validation
├── sync.py                # Dual-mode switching and backlog catch-up
├── cloud_server.py        # Cloud ingestion receiver & online processor
├── mock_service.py        # Python mock service matching biosignal-mock spec
├── pipeline.py            # Unified CLI implementation
├── pipeline               # Linux executable wrapper
├── pipeline.bat           # Windows executable wrapper
├── verify.py              # Automated 9-point validation & test suite
├── resource_monitor.py    # 1-hour resource monitoring (CPU, RAM, Disk, Net)
├── DESIGN_NOTE.md         # 2-page engineering design note (Deliverable 2)
├── FAILURE_TESTS.md       # Evidence of failure tests and results (Deliverable 3)
└── demo_guide.md          # Step-by-step 5-minute video demo script (Deliverable 5)
```

---

## 3. Installation & Setup

### Requirements
* Python 3.8+ (Zero external dependencies required; uses Python standard library).
* Linux ARM64 (Raspberry Pi 4 / Raspberry Pi OS 64-bit / Ubuntu ARM64) or macOS / Windows.

```bash
# Clone the repository
git clone https://github.com/sakshi-singh013/mayacardio-pipeline.git
cd mayacardio-pipeline

# Ensure wrapper script is executable (Linux/macOS)
chmod +x pipeline
```

---

## 4. Quick Start (Local or Device Execution)

### Step 1: Start Cloud Receiver
Run on your cloud VM or in a separate terminal:
```bash
python3 cloud_server.py --port 8000
```

### Step 2: Start Mock Service
Run `biosignal-mock` (or the included Python mock implementation):
```bash
python3 mock_service.py --mode signal --port 7100
```

### Step 3: Start the Device Pipeline
```bash
./pipeline run
```

---

## 5. Required CLI Commands

### 1. Pipeline Status
Prints current mode, samples saved on device, samples in cloud, and backlog as **one single JSON line**:
```bash
./pipeline status
```
**Example Output:**
```json
{"mode": "online", "samples_saved": 120400, "samples_in_cloud": 120400, "backlog": 0}
```

### 2. Pipeline Export
Exports data as JSON lines (one per sample, then one line per loss record):
```bash
# Export from local device disk:
./pipeline export --session <SESSION_ID> --from device

# Export from cloud copy:
./pipeline export --session <SESSION_ID> --from cloud
```

### 3. Pipeline Results
Outputs Section 3 window processing results as JSON lines:
```bash
./pipeline results --session <SESSION_ID>
```
**Example Output:**
```json
{"window": 0, "session_id": "172f7d1e", "mode": "online", "missing_1": 0, "missing_2": 0, "complete": true, "mean": {"sensor_1": [42122.2, 70204.4, 91265.3], "sensor_2": [402330.6, 157668.1]}, "min": {"sensor_1": [19167, 31945, 41528], "sensor_2": [400000, 157015]}, "max": {"sensor_1": [390042, 650070, 845091], "sensor_2": [404624, 158000]}, "magnitude": 432122.3, "heart_rate": null}
```

---

## 6. Running the Automated Verification Suite

To verify all 9 core requirements (connection, CRC-32 validation, crash durability, mode switching in $< 10\text{ s}$, backlog catch-up, and CLI outputs):
```bash
python3 verify.py
```

---

## 7. Resource Benchmarking (1-Hour Test)

To measure CPU %, peak memory, disk writes, and network usage over a 1-hour run for Deliverable 4:
```bash
python3 resource_monitor.py --duration 3600
```
Results will be output to `resource_usage.json`.

---

## 8. Author
* **Sakshi Singh** — B.Tech Computer Science and Engineering, Vellore Institute of Technology.
* **Email:** sakshisingh67117@gmail.com
