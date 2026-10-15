"""Configuration parameters for the MayaCardio data pipeline."""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Mock Service (biosignal-mock)
MOCK_HOST = os.getenv("MOCK_HOST", "127.0.0.1")
MOCK_PORT = int(os.getenv("MOCK_PORT", "7100"))

# Sampling Specification
SAMPLE_RATE = 400  # 400 samples/sec
WINDOW_SIZE = 400  # 1-second window = 400 samples
HEART_RATE_LOOKBACK_SAMPLES = 800  # 2 seconds lookback for peak validation
MIN_PEAK_DISTANCE_SAMPLES = 100    # minimum samples between peaks (0.25s)
HEART_RATE_WINDOW_SAMPLES = 2000   # 5-second window for HR calculation

# Local Storage
DEVICE_DB_PATH = os.getenv("DEVICE_DB_PATH", str(BASE_DIR / "device_pipeline.db"))
CLOUD_DB_PATH = os.getenv("CLOUD_DB_PATH", str(BASE_DIR / "cloud_pipeline.db"))

# Cloud Service
CLOUD_URL = os.getenv("CLOUD_URL", "http://127.0.0.1:8000")
CLOUD_HOST = os.getenv("CLOUD_HOST", "0.0.0.0")
CLOUD_PORT = int(os.getenv("CLOUD_PORT", "8000"))

# Network & Switching
NETWORK_TIMEOUT_SECONDS = 5.0       # Timeout for cloud healthcheck/push
OFFLINE_SWITCH_THRESHOLD = 2        # Consecutive failures before switching offline (< 10s)
ONLINE_CHECK_INTERVAL = 2.0         # Ping interval when offline (< 10s)
CATCHUP_BATCH_SIZE = 1000           # Samples per upload batch during catch-up
MAX_DATA_LOSS_POWER_CUT_SEC = 0.5   # Maximum data loss on sudden power cut
