"""Shared storage location and process ownership; no scheduler at import time."""
from contextlib import contextmanager
import os
from pathlib import Path


def database_path():
    return Path(os.environ.get('AIOPS_MONITOR_DB') or
                Path(__file__).resolve().parents[2] / 'data' / 'monitoring' / 'monitors.sqlite3').resolve()


@contextmanager
def exclusive_worker(path):
    """OS releases this advisory lock on process exit, including crashes.

    Local/same-filesystem single-worker protection, NOT distributed leases.
    Never delete the lock file: contenders must lock the same inode.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+b') as lock:
        lock.seek(0, 2)
        if lock.tell() == 0:
            lock.write(b'0')
            lock.flush()
        lock.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            lock.seek(0)
            if os.name == 'nt':
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
