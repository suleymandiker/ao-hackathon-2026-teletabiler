# -*- coding: utf-8 -*-
"""Verified segmentation policy registry.

One logical component with two implementation tiers:
- RAM: fast process-local lookup
- SQLite: persistent verified policies across restarts

Only validated segmentation policies are stored. Raw logs and raw AI responses
are never persisted here.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import Any, Dict, Optional, Tuple


class SegmentationPolicyRegistry:
    _memory: Dict[str, Dict[str, Any]] = {}
    _lock = threading.RLock()

    def __init__(self, db_path: Optional[str] = None, max_entries: int = 5000):
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.db_path = db_path or os.getenv(
            "AIOPS_POLICY_REGISTRY_PATH",
            os.path.join(root, "data", "policy_registry.sqlite3"),
        )
        self.max_entries = max(100, int(max_entries))
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path, timeout=3.0)

    def _init_db(self) -> None:
        with self._connect() as con:
            con.execute("DROP TABLE IF EXISTS policies")  # remove v43 legacy payload table
            con.execute(
                """CREATE TABLE IF NOT EXISTS segmentation_policies (
                    signature TEXT PRIMARY KEY,
                    regex TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence REAL NOT NULL DEFAULT 0,
                    coverage_score REAL NOT NULL DEFAULT 0,
                    parser_success REAL NOT NULL DEFAULT 0,
                    event_count INTEGER NOT NULL DEFAULT 0,
                    zero_loss INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    last_used REAL NOT NULL,
                    hits INTEGER NOT NULL DEFAULT 0
                )"""
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_segmentation_policy_last_used "
                "ON segmentation_policies(last_used)"
            )

    def get(self, signature: str) -> Tuple[Optional[Dict[str, Any]], str]:
        with self._lock:
            policy = self._memory.get(signature)
            if policy is not None:
                return dict(policy), "memory"

        with self._connect() as con:
            row = con.execute(
                "SELECT regex, source, confidence, coverage_score, parser_success, event_count, zero_loss, created_at "
                "FROM segmentation_policies WHERE signature=?",
                (signature,),
            ).fetchone()
            if row is None:
                return None, "miss"
            now = time.time()
            con.execute(
                "UPDATE segmentation_policies SET last_used=?, hits=hits+1 WHERE signature=?",
                (now, signature),
            )

        policy = {
            "verified": True,
            "regex": row[0],
            "source": row[1],
            "confidence": float(row[2] or 0.0),
            "coverage_score": float(row[3] or 0.0),
            "parser_success": float(row[4] or 0.0),
            "event_count": int(row[5] or 0),
            "zero_loss": bool(row[6]),
            "created_at": float(row[7]),
        }
        with self._lock:
            self._memory[signature] = policy
        return dict(policy), "sqlite"

    def put(self, signature: str, policy: Dict[str, Any]) -> None:
        """Persist only a verified, compact segmentation policy."""
        if not policy.get("verified") or not policy.get("regex"):
            raise ValueError("Only verified policies with a regex may be stored")

        validation = policy.get("validation") or {}
        parser_validation = validation.get("parser_validation") or {}
        compact = {
            "verified": True,
            "regex": str(policy["regex"]),
            "source": str(policy.get("source", "unknown")),
            "confidence": float(policy.get("confidence", 0.0) or 0.0),
            "coverage_score": float(
                validation.get("coverage_score", policy.get("coverage_score", 0.0)) or 0.0
            ),
            "parser_success": float(
                parser_validation.get(
                    "success_ratio", policy.get("parser_success", 0.0)
                )
                or 0.0
            ),
            "event_count": int(
                validation.get("event_count", policy.get("event_count", 0)) or 0
            ),
            "zero_loss": bool(
                validation.get(
                    "accounting_zero_loss", policy.get("zero_loss", False)
                )
            ),
        }
        now = time.time()
        with self._connect() as con:
            existing = con.execute(
                "SELECT created_at FROM segmentation_policies WHERE signature=?",
                (signature,),
            ).fetchone()
            created_at = float(existing[0]) if existing else now
            con.execute(
                """INSERT INTO segmentation_policies(
                    signature, regex, source, confidence, coverage_score,
                    parser_success, event_count, zero_loss, created_at, updated_at, last_used, hits
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,0)
                ON CONFLICT(signature) DO UPDATE SET
                    regex=excluded.regex,
                    source=excluded.source,
                    confidence=excluded.confidence,
                    coverage_score=excluded.coverage_score,
                    parser_success=excluded.parser_success,
                    event_count=excluded.event_count,
                    zero_loss=excluded.zero_loss,
                    updated_at=excluded.updated_at,
                    last_used=excluded.last_used""",
                (
                    signature,
                    compact["regex"],
                    compact["source"],
                    compact["confidence"],
                    compact["coverage_score"],
                    compact["parser_success"],
                    compact["event_count"],
                    int(compact["zero_loss"]),
                    created_at,
                    now,
                    now,
                ),
            )
            count = con.execute(
                "SELECT COUNT(*) FROM segmentation_policies"
            ).fetchone()[0]
            if count > self.max_entries:
                con.execute(
                    "DELETE FROM segmentation_policies WHERE signature IN "
                    "(SELECT signature FROM segmentation_policies ORDER BY last_used ASC LIMIT ?)",
                    (count - self.max_entries,),
                )

        compact["created_at"] = created_at
        with self._lock:
            self._memory[signature] = compact

    @classmethod
    def clear_memory(cls) -> None:
        with cls._lock:
            cls._memory.clear()
