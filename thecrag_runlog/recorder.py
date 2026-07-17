"""Rolling per-task run-log with atomic JSON file output.

Usage:

    from datetime import datetime, timezone
    from pathlib import Path
    from thecrag_runlog import Recorder, RunRecord

    recorder = Recorder(
        service_name="my-service",
        output_path=Path("/data/status/status.json"),
        max_runs_per_task=20,
    )

    started = datetime.now(timezone.utc)
    # ... do work ...
    recorder.record(RunRecord(
        task_name="my-task",
        started_at=started,
        finished_at=datetime.now(timezone.utc),
        processed=42,
        failed=0,
        error=None,
    ))

The JSON file is rewritten atomically (write-to-temp + os.replace) after every
record() call, so a reverse proxy serving the file as static content never
sees a partial write.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RunRecord:
    """One execution of a worker task."""

    task_name: str
    started_at: datetime  # tz-aware UTC
    finished_at: datetime  # tz-aware UTC
    processed: int
    failed: int
    error: Optional[str] = None
    # Free-form identifier of what triggered this run — HTTP services typically
    # populate it with the inbound URL + query string; batch workers can use it
    # for a job id, cron expression, etc.
    call: Optional[str] = None

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()

    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at.isoformat(timespec="seconds"),
            "finished_at": self.finished_at.isoformat(timespec="seconds"),
            "duration_seconds": round(self.duration_seconds, 1),
            "processed": self.processed,
            "failed": self.failed,
            "error": self.error,
            "call": self.call,
        }


def _read_version_file() -> str:
    try:
        return (Path.cwd() / ".version").read_text().strip() or "undefined"
    except OSError:
        return "undefined"


def _format_uptime(seconds: int) -> str:
    d, seconds = divmod(seconds, 86400)
    h, seconds = divmod(seconds, 3600)
    m, s = divmod(seconds, 60)
    parts = []
    if d:
        parts.append(f"{d}d")
    if h or d:
        parts.append(f"{h}h")
    if m or h or d:
        parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)


class Recorder:
    """Thread-safe rolling run-log writer.

    Maintains a `collections.deque(maxlen=N)` per task name, in memory. On
    every `record()` call the deque is updated and the full snapshot is
    atomically rewritten to `output_path`.
    """

    def __init__(
        self,
        service_name: str,
        output_path: Path,
        max_runs_per_task: int = 20,
        version: Optional[str] = None,
    ):
        self._service_name = service_name
        self._output_path = Path(output_path)
        self._max = max_runs_per_task
        self._version = version if version is not None else _read_version_file()
        self._started_at = datetime.now(timezone.utc)
        self._lock = threading.Lock()
        self._by_task: dict[str, deque[RunRecord]] = {}

        # Ensure the directory exists. Volume mounts in Docker will create
        # the mount point but not intermediate dirs requested by the caller.
        self._output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write an empty snapshot at startup so the file always exists.
        # A consumer hitting the URL before the first run sees an empty
        # tasks dict, not a 404.
        self._write_snapshot()

    def record(self, run: RunRecord) -> None:
        """Append a run and atomically rewrite the JSON file."""
        with self._lock:
            buf = self._by_task.setdefault(run.task_name, deque(maxlen=self._max))
            buf.append(run)
        # Write outside the lock — _write_snapshot takes its own snapshot
        # under the lock again to keep the critical section small.
        self._write_snapshot()

    def _snapshot_dict(self) -> dict:
        with self._lock:
            tasks = {
                name: [r.to_dict() for r in reversed(buf)]
                for name, buf in self._by_task.items()
            }
        now = datetime.now(timezone.utc)
        return {
            "service": self._service_name,
            "version": self._version,
            "uptime": _format_uptime(int((now - self._started_at).total_seconds())),
            "generated_at": now.isoformat(timespec="seconds"),
            "tasks": tasks,
        }

    def _write_snapshot(self) -> None:
        """Atomically rewrite the status file (write-to-temp + os.replace)."""
        snapshot = self._snapshot_dict()
        tmp = self._output_path.with_suffix(self._output_path.suffix + ".tmp")
        try:
            tmp.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
            os.replace(tmp, self._output_path)
        except OSError as e:
            logger.warning(f"Failed to write run-log to {self._output_path}: {e}")
            # Best-effort cleanup of the tmp file if rename failed.
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
