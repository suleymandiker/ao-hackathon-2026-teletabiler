"""Exact run-local reference dedupe outside the shared monitoring database."""

from contextlib import contextmanager
from pathlib import Path
import sqlite3
import tempfile


MAX_MEMORY_REFERENCES = 100_000


class IsolatedRunLedger:
    def __init__(self, monitor_db_path, run_id, monitor_id, since, receipt_cutoff):
        self.monitor_db_path = Path(monitor_db_path)
        self.run_id = run_id
        self.monitor_id = monitor_id
        self.since = since
        self.receipt_cutoff = receipt_cutoff
        self.memory = {}
        self.local = None
        self.temp = None
        self.local_path = None
        self.receipts = None

    def __enter__(self):
        self.receipts = sqlite3.connect(self.monitor_db_path.resolve().as_uri() + '?mode=ro',
                                        uri=True, timeout=10)
        self.receipts.execute('PRAGMA query_only=ON')
        self.receipts.execute('PRAGMA busy_timeout=10000')
        return self

    def __exit__(self, error_type, error, traceback):
        if self.local is not None:
            if self.local.in_transaction:
                self.local.rollback()
            self.local.close()
            self.local = None
        if self.receipts is not None:
            self.receipts.close()
            self.receipts = None
        if error_type is not None:
            self.cleanup()

    def _spill(self):
        parent = self.monitor_db_path.parent / 'tmp'
        parent.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix=f'run-{self.run_id}-', dir=parent)
        self.local_path = Path(self.temp.name) / 'references.sqlite3'
        self.local = sqlite3.connect(self.local_path, timeout=10)
        self.local.execute('CREATE TABLE refs (key TEXT PRIMARY KEY, source_time TEXT NOT NULL, owned INTEGER NOT NULL)')
        self.local.executemany('INSERT INTO refs VALUES (?,?,?)',
                               ((key, value[0], int(value[1])) for key, value in self.memory.items()))
        self.memory.clear()

    @contextmanager
    def page(self):
        try:
            yield
            if self.local is not None:
                self.local.commit()
        except BaseException:
            if self.local is not None:
                self.local.rollback()
            raise

    def seen(self, key, source_time):
        if self.local is None:
            if key in self.memory:
                return True
            if len(self.memory) < MAX_MEMORY_REFERENCES:
                self.memory[key] = (source_time, False)
                return False
            self._spill()
        row = self.local.execute('INSERT OR IGNORE INTO refs VALUES (?,?,0)',
                                 (key, source_time))
        return row.rowcount == 0

    def consumed(self, key):
        return self.receipts.execute('''SELECT 1 FROM monitor_receipts
            WHERE monitor_id=? AND reference_hash=? AND source_time>=?''',
            (self.monitor_id, key, self.since)).fetchone() is not None

    def owned(self, key):
        if self.local is None:
            return self.memory.get(key, (None, False))[1]
        row = self.local.execute('SELECT owned FROM refs WHERE key=?', (key,)).fetchone()
        return row is not None and row[0] == 1

    def mark_owned(self, keys):
        if self.local is None:
            for key in keys:
                if key in self.memory:
                    self.memory[key] = (self.memory[key][0], True)
            return
        self.local.executemany('UPDATE refs SET owned=1 WHERE key=?', ((key,) for key in keys))

    def items(self):
        if self.local_path is None:
            return ((key, stamp) for key, (stamp, owned) in self.memory.items()
                    if owned and stamp >= self.receipt_cutoff)

        def rows():
            db = sqlite3.connect(self.local_path.resolve().as_uri() + '?mode=ro', uri=True)
            try:
                yield from db.execute('SELECT key,source_time FROM refs WHERE owned=1 AND source_time>=?',
                                      (self.receipt_cutoff,))
            finally:
                db.close()
        return rows()

    def cleanup(self):
        if self.temp is not None:
            self.temp.cleanup()
            self.temp = None
