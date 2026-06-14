"""Reusable run-log + heartbeat primitives for theCrag background workers.

Public API:

- ``Recorder`` / ``RunRecord`` — rolling per-task run log with atomic JSON output
- ``touch_heartbeat`` — writes the heartbeat file the Docker healthcheck reads

The CLI healthcheck is at ``python -m thecrag_runlog.healthcheck``.
"""

from thecrag_runlog.heartbeat import DEFAULT_HEARTBEAT_PATH, touch_heartbeat
from thecrag_runlog.recorder import Recorder, RunRecord

__all__ = [
    "Recorder",
    "RunRecord",
    "touch_heartbeat",
    "DEFAULT_HEARTBEAT_PATH",
]
