"""Heartbeat-file helpers shared by the writer (service main loop) and the
reader (the CLI healthcheck).

The default path is ``/tmp/heartbeat`` — services SHOULD override this
to a per-service name via the ``HEARTBEAT_PATH`` env var so multiple
workers running on the same host don't clobber each other's heartbeat.
Both ``touch_heartbeat()`` and ``python -m thecrag_runlog.healthcheck``
read the same env var so they always agree.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


DEFAULT_HEARTBEAT_PATH = Path(os.getenv("HEARTBEAT_PATH", "/tmp/heartbeat"))


def touch_heartbeat(path: Optional[Path] = None) -> None:
    """Update the heartbeat file's mtime.

    Call this at least once per minute from the service's main loop or
    from a dedicated APScheduler interval job. The Docker ``HEALTHCHECK``
    invocation of ``python -m thecrag_runlog.healthcheck`` reads the same
    file and fails when its mtime stops advancing.

    Args:
        path: Override the default path (otherwise reads ``HEARTBEAT_PATH``
            env or falls back to ``/tmp/heartbeat``).
    """
    target = Path(path) if path is not None else DEFAULT_HEARTBEAT_PATH
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.touch()
    except OSError as e:
        logger.warning(f"Failed to update heartbeat at {target}: {e}")
