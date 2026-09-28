"""Reusable observability primitives for theCrag background workers.

Public API:

- ``Recorder`` / ``RunRecord`` — rolling per-task run log with atomic JSON output
- ``touch_heartbeat`` — writes the heartbeat file the Docker healthcheck reads
- ``track_event`` / ``flush_events`` — fire-and-forget analytics events

The CLI healthcheck is at ``python -m thecrag_runlog.healthcheck``.

``track_event`` is inert unless ``ANALYTICS_URL`` is set, so importing this
package costs nothing for services that don't use analytics.
"""

from thecrag_runlog.analytics import flush_events, track_event
from thecrag_runlog.heartbeat import DEFAULT_HEARTBEAT_PATH, touch_heartbeat
from thecrag_runlog.recorder import Recorder, RunRecord

__all__ = [
    "Recorder",
    "RunRecord",
    "touch_heartbeat",
    "DEFAULT_HEARTBEAT_PATH",
    "track_event",
    "flush_events",
]
