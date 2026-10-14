# Pipeline Project Structure
# Core Modules:
# - config.py: Configuration settings (ports, URLs, intervals, database paths)
# - storage.py: SQLite-based crash-resilient edge storage with WAL mode
# - processor.py: Window aggregation & heart-rate peak calculation (shared online/offline)
# - client.py: TCP client reading from biosignal-mock with CRC-32 checking and auto-recovery
# - sync.py: Edge-to-cloud streaming, offline detection, and catch-up synchronization
# - cloud_server.py: Cloud-side receiver and processor
# - mock_service.py: Python implementation of biosignal-mock (modes: signal, ramp) for local development/testing
# - pipeline: Unified CLI executable fulfilling `pipeline status`, `pipeline export`, and `pipeline results`
