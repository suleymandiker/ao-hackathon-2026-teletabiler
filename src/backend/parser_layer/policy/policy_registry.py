# -*- coding: utf-8 -*-
import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from data_paths import policy_data_dir


class ParserPolicyRegistry:
    """Small RAM + SQLite registry for verified custom parser policies.

    SQLite is control-plane only. The hot event path never performs a lookup.
    """

    def __init__(self, db_path=None):
        default_db_path = policy_data_dir() / "policy_registry.sqlite3"
        self.db_path = Path(
            db_path
            or os.getenv("AIOPS_POLICY_REGISTRY_PATH")
            or default_db_path
        )
        self._memory = {}
        self._lock = threading.Lock()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self):
        return sqlite3.connect(str(self.db_path), timeout=10)

    def _init_db(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS parser_policies (
                    signature TEXT PRIMARY KEY,
                    policy_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    verified INTEGER NOT NULL,
                    validation_success REAL NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    last_used REAL NOT NULL,
                    hits INTEGER NOT NULL DEFAULT 0
                )
            """)

    def get(self, signature):
        policy = self._memory.get(signature)
        if policy is not None:
            return dict(policy), "memory"
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT policy_json FROM parser_policies WHERE signature=? AND verified=1",
                (signature,),
            ).fetchone()
            if not row:
                return None, None
            now = time.time()
            conn.execute(
                "UPDATE parser_policies SET hits=hits+1,last_used=? WHERE signature=?",
                (now, signature),
            )
        policy = json.loads(row[0])
        self._memory[signature] = policy
        return dict(policy), "sqlite"

    def save(self, signature, policy, source, validation_success=1.0):
        now = time.time()
        payload = json.dumps(policy, ensure_ascii=False, separators=(",", ":"))
        with self._lock, self._connect() as conn:
            conn.execute("""
                INSERT INTO parser_policies
                    (signature,policy_json,source,verified,validation_success,created_at,updated_at,last_used,hits)
                VALUES (?,?,?,?,?,?,?,?,0)
                ON CONFLICT(signature) DO UPDATE SET
                    policy_json=excluded.policy_json,
                    source=excluded.source,
                    verified=excluded.verified,
                    validation_success=excluded.validation_success,
                    updated_at=excluded.updated_at,
                    last_used=excluded.last_used
            """, (signature, payload, source, 1, float(validation_success), now, now, now))
        self._memory[signature] = dict(policy)

    def invalidate(self, signature):
        """Remove one learned policy from RAM + SQLite for explicit cold-path QA."""
        if not signature:
            return False
        self._memory.pop(signature, None)
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM parser_policies WHERE signature=?",
                (signature,),
            )
        return bool(cur.rowcount)
