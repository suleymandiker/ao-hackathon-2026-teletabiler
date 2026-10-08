"""Temporary local pytest adapter for the Windows sandbox's 0700 ACL mismatch."""

from pathlib import Path
import tempfile
from uuid import uuid4

from _pytest.tmpdir import TempPathFactory


ROOT = (Path(__file__).parent / 'tests' / 'pytest_fixed').resolve()


def getbasetemp(self):
    ROOT.mkdir(mode=0o777, parents=True, exist_ok=True)
    self._basetemp = ROOT
    return ROOT


def mktemp(self, basename, numbered=True):
    name = self._ensure_relative_to_basetemp(basename)
    target = ROOT / (name + uuid4().hex if numbered else name)
    target.mkdir(mode=0o777)
    return target


TempPathFactory.getbasetemp = getbasetemp
TempPathFactory.mktemp = mktemp


def sandbox_mkdtemp(suffix=None, prefix=None, dir=None):
    parent = Path(dir) if dir is not None else ROOT
    parent.mkdir(mode=0o777, parents=True, exist_ok=True)
    target = parent / ((prefix or 'tmp-') + uuid4().hex + (suffix or ''))
    target.mkdir(mode=0o777)
    return str(target)


tempfile.mkdtemp = sandbox_mkdtemp
