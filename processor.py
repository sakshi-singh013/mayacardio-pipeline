"""Processing engine for 1-second window aggregation and heart-rate detection.

Implements the exact rules specified in Section 3 of the Case Assessment.
The exact same logic is executed on the device (offline mode) and in the cloud (online mode).
"""
import math
from typing import Dict, List, Any, Optional
from config import SAMPLE_RATE, WINDOW_SIZE


def compute_peaks_for_session(ch2_dict: Dict[int, int], max_index: int) -> List[int]:
    """Detect peaks in sensor 1, channel 2 up to max_index.
    
    A sample i is a peak when all four conditions hold:
    1. The 800 samples before it, and the sample after it, are available (i-800 to i+1).
    2. It is larger than both neighbours: ch2[i] > ch2[i-1] and ch2[i] > ch2[i+1].
    3. It is larger than half of the maximum of the 800 samples before it:
       ch2[i] > 0.5 * max(ch2[i-800 ... i-1]).
    4. It is at least 100 samples after the previous peak (i - prev_peak >= 100).
    """
    peaks = []
    prev_peak = -1000

    # Start searching from 800 since 800 preceding samples are required
    for i in range(800, max_index):
        curr_val = ch2_dict.get(i)
        left_val = ch2_dict.get(i - 1)
        right_val = ch2_dict.get(i + 1)
        if curr_val is None or left_val is None or right_val is None:
            continue
        
        # Condition 2: larger than both neighbours
        if not (curr_val > left_val and curr_val > right_val):
            continue

        # Condition 4: at least 100 samples after previous peak
        if (i - prev_peak) < 100:
            continue

        # Condition 1 & 3: Check prior 800 samples exist and find max
        prior_samples = [ch2_dict.get(p) for p in range(i - 800, i)]
        if any(v is None for v in prior_samples):
            continue

        prior_max = max(prior_samples)

        # Condition 3: larger than half of max of 800 samples before it
        if curr_val > (0.5 * prior_max):
            peaks.append(i)
            prev_peak = i

    return peaks


def process_window(
    session_id: str,
    window: int,
    mode: str,
    s1_samples: Dict[int, List[int]],
    s2_samples: Dict[int, List[int]],
    s1_ch2_history: Dict[int, int]
) -> Dict[str, Any]:
    """Process a single 1-second window w (samples 400w to 400w + 399)."""
    start_idx = 400 * window
    end_idx = 400 * window + 399

    # Count missing samples
    missing_1 = 0
    missing_2 = 0
    for idx in range(start_idx, end_idx + 1):
        if idx not in s1_samples:
            missing_1 += 1
        if idx not in s2_samples:
            missing_2 += 1

    complete = (missing_1 == 0 and missing_2 == 0)

    # Sensor 1 statistics (3 channels)
    s1_available = [s1_samples[i] for i in range(start_idx, end_idx + 1) if i in s1_samples]
    if s1_available:
        mean_s1 = [
            round(sum(s[c] for s in s1_available) / len(s1_available), 1)
            for c in range(3)
        ]
        min_s1 = [min(s[c] for s in s1_available) for c in range(3)]
        max_s1 = [max(s[c] for s in s1_available) for c in range(3)]
    else:
        mean_s1 = None
        min_s1 = None
        max_s1 = None

    # Sensor 2 statistics (2 channels)
    s2_available = [s2_samples[i] for i in range(start_idx, end_idx + 1) if i in s2_samples]
    if s2_available:
        mean_s2 = [
            round(sum(s[c] for s in s2_available) / len(s2_available), 1)
            for c in range(2)
        ]
        min_s2 = [min(s[c] for s in s2_available) for c in range(2)]
        max_s2 = [max(s[c] for s in s2_available) for c in range(2)]
        
        # Magnitude: Mean of sqrt(ch1^2 + ch2^2)
        total_mag = sum(math.hypot(s[0], s[1]) for s in s2_available)
        magnitude = round(total_mag / len(s2_available), 1)
    else:
        mean_s2 = None
        min_s2 = None
        max_s2 = None
        magnitude = None

    mean_stat = {"sensor_1": mean_s1, "sensor_2": mean_s2} if (mean_s1 or mean_s2) else None
    min_stat = {"sensor_1": min_s1, "sensor_2": min_s2} if (min_s1 or min_s2) else None
    max_stat = {"sensor_1": max_s1, "sensor_2": max_s2} if (max_s1 or max_s2) else None

    # Heart Rate Calculation
    # Rule 3: heart_rate is null if w < 4, if any sample in those 5 seconds is missing, or if fewer than 2 peaks
    heart_rate = None
    if window >= 4:
        hr_start = 400 * (window - 4)
        hr_end = 400 * (window + 1) - 1

        # Check if any sample in the 5-second interval is missing
        all_samples_present = all((idx in s1_ch2_history) for idx in range(hr_start, hr_end + 1))
        
        if all_samples_present:
            # Detect peaks in the session up to hr_end + 1
            all_peaks = compute_peaks_for_session(s1_ch2_history, hr_end + 1)
            # Filter peaks that fall inside the 5-second window [hr_start, hr_end]
            window_peaks = [p for p in all_peaks if hr_start <= p <= hr_end]

            if len(window_peaks) >= 2:
                # Mean gap between consecutive peaks in seconds
                # = (p_last - p_first) / (400 * (count - 1))
                total_gap_seconds = (window_peaks[-1] - window_peaks[0]) / float(SAMPLE_RATE)
                mean_gap_seconds = total_gap_seconds / (len(window_peaks) - 1)
                if mean_gap_seconds > 0:
                    heart_rate = round(60.0 / mean_gap_seconds, 1)

    return {
        "session_id": session_id,
        "window": window,
        "mode": mode,
        "missing_1": missing_1,
        "missing_2": missing_2,
        "complete": complete,
        "mean": mean_stat,
        "min": min_stat,
        "max": max_stat,
        "magnitude": magnitude,
        "heart_rate": heart_rate
    }
