"""Docker HEALTHCHECK CLI for theCrag background-worker services.

Exits 0 if the heartbeat file exists and is younger than
``HEARTBEAT_MAX_AGE_SECONDS`` (default 300). Exits 1 otherwise — which
flips the container to 'unhealthy' after the configured retries in the
Dockerfile HEALTHCHECK directive.

Use in a Dockerfile via:

    HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \\
        CMD ["python", "-m", "thecrag_runlog.healthcheck"]

The heartbeat file is touched once per minute by the service's run loop
(see ``thecrag_runlog.touch_heartbeat``). If the scheduler wedges or the
main thread dies, the mtime stops advancing and this script trips.
"""

from __future__ import annotations

import os
import sys
import time

from thecrag_runlog.heartbeat import DEFAULT_HEARTBEAT_PATH


MAX_AGE_SECONDS = int(os.getenv("HEARTBEAT_MAX_AGE_SECONDS", "300"))  # 5 min default


def main() -> int:
    if not DEFAULT_HEARTBEAT_PATH.exists():
        print(f"heartbeat file missing: {DEFAULT_HEARTBEAT_PATH}", file=sys.stderr)
        return 1

    age = time.time() - DEFAULT_HEARTBEAT_PATH.stat().st_mtime
    if age > MAX_AGE_SECONDS:
        print(
            f"heartbeat stale: {age:.0f}s old (max {MAX_AGE_SECONDS}s)",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
