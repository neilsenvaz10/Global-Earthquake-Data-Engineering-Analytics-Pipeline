"""Phase 2 local staging: atomic catalog + checkpoints, with one OS file lock.

This is not a PostgreSQL loader. State is deliberately simple and kept in memory.
"""
import json
import os
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4


def atomic_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + str(uuid4()) + ".tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def staging_lock(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".writer.lock").open("a+b") as stream:
        stream.seek(0, 2)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        if os.name == "nt":
            import msvcrt
            try:
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                raise RuntimeError("Another ETL process owns this staging directory") from None
        else:
            import fcntl
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise RuntimeError("Another ETL process owns this staging directory") from None
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)


def load_state(root: Path):
    path = root / "state.json"
    if not path.exists():
        return {"contract_version": 1, "catalog": {}, "checkpoints": {}}
    state = json.loads(path.read_text(encoding="utf-8"))
    if state.get("contract_version") != 1 or not isinstance(state.get("catalog"), dict) or not isinstance(state.get("checkpoints"), dict):
        raise ValueError("Unsupported or malformed staging state; do not overwrite it")
    return state
