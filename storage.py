"""Storage engine for edge device and cloud receiver.

Provides crash-resilient, ACID-compliant persistence using SQLite with WAL mode.
Every sample is saved to disk immediately, surviving process kills, reboots, and sudden power cuts.
"""
import sqlite3
import json
import time
from typing import List, Tuple, Dict, Any, Optional, Generator


class PipelineStorage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        # Enable Write-Ahead Logging (WAL) for high concurrency and crash durability
        conn.execute("PRAGMA journal_mode = WAL;")
        # FULL synchronous ensures OS flushes write buffers on commit (survives power pulls)
        conn.execute("PRAGMA synchronous = FULL;")
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS samples (
                session_id TEXT NOT NULL,
                sensor INTEGER NOT NULL,
                sample_index INTEGER NOT NULL,
                ch1 INTEGER NOT NULL,
                ch2 INTEGER NOT NULL,
                ch3 INTEGER,
                synced INTEGER DEFAULT 0,
                PRIMARY KEY (session_id, sensor, sample_index)
            );

            CREATE INDEX IF NOT EXISTS idx_samples_sync 
            ON samples (session_id, synced, sensor, sample_index);

            CREATE INDEX IF NOT EXISTS idx_samples_lookup 
            ON samples (session_id, sensor, sample_index);

            CREATE INDEX IF NOT EXISTS idx_samples_synced_flag 
            ON samples (synced, session_id, sensor, sample_index);

            CREATE TABLE IF NOT EXISTS loss_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                sensor INTEGER NOT NULL,
                from_index INTEGER NOT NULL,
                to_index INTEGER,
                reason TEXT NOT NULL,
                created_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS results (
                session_id TEXT NOT NULL,
                window INTEGER NOT NULL,
                mode TEXT NOT NULL,
                missing_1 INTEGER NOT NULL,
                missing_2 INTEGER NOT NULL,
                complete INTEGER NOT NULL,
                mean_json TEXT,
                min_json TEXT,
                max_json TEXT,
                magnitude REAL,
                heart_rate REAL,
                created_at REAL NOT NULL,
                PRIMARY KEY (session_id, window, mode)
            );

            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """)

    def set_meta(self, key: str, value: str):
        with self._get_connection() as conn:
            conn.execute(
                "INSERT INTO metadata (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value;",
                (key, value)
            )

    def get_meta(self, key: str, default: Optional[str] = None) -> Optional[str]:
        with self._get_connection() as conn:
            row = conn.execute("SELECT value FROM metadata WHERE key = ?;", (key,)).fetchone()
            return row["value"] if row else default

    def insert_samples(self, session_id: str, sensor: int, first_index: int, data: List[List[int]], synced: bool = False) -> int:
        """Insert batch of samples into storage.
        
        Uses INSERT OR IGNORE to automatically reject duplicate samples sent by mock or network retries.
        """
        if not data:
            return 0
        
        rows = []
        synced_int = 1 if synced else 0
        for offset, vals in enumerate(data):
            idx = first_index + offset
            ch1 = vals[0]
            ch2 = vals[1]
            ch3 = vals[2] if len(vals) > 2 else None
            rows.append((session_id, sensor, idx, ch1, ch2, ch3, synced_int))

        with self._get_connection() as conn:
            cursor = conn.executemany(
                "INSERT OR IGNORE INTO samples "
                "(session_id, sensor, sample_index, ch1, ch2, ch3, synced) "
                "VALUES (?, ?, ?, ?, ?, ?, ?);",
                rows
            )
            return cursor.rowcount

    def record_loss(self, session_id: str, sensor: int, from_index: int, to_index: Optional[int], reason: str):
        """Record sample loss explicitly (no silent loss allowed)."""
        with self._get_connection() as conn:
            conn.execute(
                "INSERT INTO loss_records (session_id, sensor, from_index, to_index, reason, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?);",
                (session_id, sensor, from_index, to_index, reason, time.time())
            )

    def get_unsynced_batch(self, limit: int = 2000) -> List[Dict[str, Any]]:
        """Fetch unsynced samples for cloud streaming/catchup."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT session_id, sensor, sample_index, ch1, ch2, ch3 "
                "FROM samples WHERE synced = 0 "
                "ORDER BY session_id, sensor, sample_index LIMIT ?;",
                (limit,)
            ).fetchall()
            
            result = []
            for r in rows:
                vals = [r["ch1"], r["ch2"]]
                if r["ch3"] is not None:
                    vals.append(r["ch3"])
                result.append({
                    "session_id": r["session_id"],
                    "sensor": r["sensor"],
                    "index": r["sample_index"],
                    "values": vals
                })
            return result

    def mark_synced(self, items: List[Dict[str, Any]]):
        """Mark uploaded samples as synced using fast range updates."""
        if not items:
            return
        
        # Group by (session_id, sensor) into continuous index ranges
        from collections import defaultdict
        grouped = defaultdict(list)
        for it in items:
            grouped[(it["session_id"], it["sensor"])].append(it["index"])

        range_tuples = []
        for (sess, sensor), indices in grouped.items():
            indices.sort()
            start = indices[0]
            prev = start
            for idx in indices[1:]:
                if idx == prev + 1:
                    prev = idx
                else:
                    range_tuples.append((sess, sensor, start, prev))
                    start = idx
                    prev = idx
            range_tuples.append((sess, sensor, start, prev))

        with self._get_connection() as conn:
            conn.executemany(
                "UPDATE samples SET synced = 1 WHERE session_id = ? AND sensor = ? AND sample_index BETWEEN ? AND ?;",
                range_tuples
            )

    def get_status_counts(self, session_id: Optional[str] = None) -> Dict[str, int]:
        """Calculate total saved, cloud-synced, and backlog samples."""
        with self._get_connection() as conn:
            if session_id:
                total_row = conn.execute(
                    "SELECT COUNT(*) AS total, SUM(synced) AS synced_count "
                    "FROM samples WHERE session_id = ?;", (session_id,)
                ).fetchone()
            else:
                total_row = conn.execute(
                    "SELECT COUNT(*) AS total, SUM(synced) AS synced_count FROM samples;"
                ).fetchone()
            
            total = total_row["total"] or 0
            synced = total_row["synced_count"] or 0
            backlog = total - synced
            return {
                "samples_saved": total,
                "samples_in_cloud": synced,
                "backlog": backlog
            }

    def get_window_sensor_data(self, session_id: str, sensor: int, start_index: int, end_index: int) -> Dict[int, List[int]]:
        """Retrieve samples for a given sensor within [start_index, end_index].
        
        Returns mapping of sample_index -> list of channel values.
        """
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT sample_index, ch1, ch2, ch3 FROM samples "
                "WHERE session_id = ? AND sensor = ? AND sample_index BETWEEN ? AND ? "
                "ORDER BY sample_index ASC;",
                (session_id, sensor, start_index, end_index)
            ).fetchall()
            
            res = {}
            for r in rows:
                v = [r["ch1"], r["ch2"]]
                if r["ch3"] is not None:
                    v.append(r["ch3"])
                res[r["sample_index"]] = v
            return res

    def get_sensor1_ch2_slice(self, session_id: str, start_index: int, end_index: int) -> Dict[int, int]:
        """Fetch ch2 of sensor 1 for peak detection and heart rate analysis."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT sample_index, ch2 FROM samples "
                "WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ? "
                "ORDER BY sample_index ASC;",
                (session_id, start_index, end_index)
            ).fetchall()
            return {r["sample_index"]: r["ch2"] for r in rows}

    def save_window_result(self, result: Dict[str, Any]):
        """Store window processing output."""
        with self._get_connection() as conn:
            conn.execute(
                "INSERT INTO results "
                "(session_id, window, mode, missing_1, missing_2, complete, mean_json, min_json, max_json, magnitude, heart_rate, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(session_id, window, mode) DO UPDATE SET "
                "missing_1=excluded.missing_1, missing_2=excluded.missing_2, complete=excluded.complete, "
                "mean_json=excluded.mean_json, min_json=excluded.min_json, max_json=excluded.max_json, "
                "magnitude=excluded.magnitude, heart_rate=excluded.heart_rate, created_at=excluded.created_at;",
                (
                    result["session_id"],
                    result["window"],
                    result["mode"],
                    result["missing_1"],
                    result["missing_2"],
                    1 if result["complete"] else 0,
                    json.dumps(result.get("mean")),
                    json.dumps(result.get("min")),
                    json.dumps(result.get("max")),
                    result.get("magnitude"),
                    result.get("heart_rate"),
                    time.time()
                )
            )

    def export_samples_and_losses(self, session_id: str) -> Generator[str, None, None]:
        """Generator yielding JSON lines for `pipeline export`:
        First all samples, then all loss records.
        """
        with self._get_connection() as conn:
            # 1. Samples
            cursor = conn.execute(
                "SELECT sensor, sample_index, ch1, ch2, ch3 FROM samples "
                "WHERE session_id = ? ORDER BY sensor, sample_index ASC;",
                (session_id,)
            )
            for r in cursor:
                vals = [r["ch1"], r["ch2"]]
                if r["ch3"] is not None:
                    vals.append(r["ch3"])
                line = json.dumps({"sensor": r["sensor"], "index": r["sample_index"], "values": vals})
                yield line

            # 2. Losses
            loss_cursor = conn.execute(
                "SELECT sensor, from_index, to_index, reason FROM loss_records "
                "WHERE session_id = ? ORDER BY id ASC;",
                (session_id,)
            )
            for r in loss_cursor:
                line = json.dumps({
                    "sensor": r["sensor"],
                    "from_index": r["from_index"],
                    "to_index": r["to_index"],
                    "reason": r["reason"]
                })
                yield line

    def export_results(self, session_id: str) -> Generator[str, None, None]:
        """Generator yielding JSON lines for `pipeline results`."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT window, mode, missing_1, missing_2, complete, mean_json, min_json, max_json, magnitude, heart_rate "
                "FROM results WHERE session_id = ? ORDER BY window ASC, mode ASC;",
                (session_id,)
            )
            for r in cursor:
                obj = {
                    "window": r["window"],
                    "session_id": session_id,
                    "mode": r["mode"],
                    "missing_1": r["missing_1"],
                    "missing_2": r["missing_2"],
                    "complete": bool(r["complete"]),
                    "mean": json.loads(r["mean_json"]) if r["mean_json"] else None,
                    "min": json.loads(r["min_json"]) if r["min_json"] else None,
                    "max": json.loads(r["max_json"]) if r["max_json"] else None,
                    "magnitude": r["magnitude"],
                    "heart_rate": r["heart_rate"]
                }
                yield json.dumps(obj)
