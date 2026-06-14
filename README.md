# thecrag-py-runlog

Reusable run-log + heartbeat + healthcheck primitives for theCrag
background-worker services. Stdlib only, drops into any Python 3.12+
service. Designed to fit the operational shape described in
[thecrag-py-altitude-task/SERVICE_TEMPLATE.md](https://github.com/theCrag/thecrag-py-altitude-task/blob/master/SERVICE_TEMPLATE.md).

## What's in the box

| Module | Purpose |
|---|---|
| `thecrag_runlog.Recorder` | Thread-safe per-task rolling deque (last N runs). Atomically rewrites a JSON file after every record — safe to serve directly as a static asset via Caddy / nginx. |
| `thecrag_runlog.RunRecord` | Dataclass for one execution of a task (start/end timestamps, processed/failed counts, optional error). |
| `thecrag_runlog.touch_heartbeat` | Touches the heartbeat file the Docker healthcheck reads. Call once per minute from a scheduled job or main loop. |
| `python -m thecrag_runlog.healthcheck` | CLI entry point used in `Dockerfile`'s `HEALTHCHECK` directive. Exits 0/1 based on heartbeat freshness. |

## Install

```bash
pip install "thecrag-py-runlog @ git+https://github.com/theCrag/thecrag-py-runlog.git@<SHA>"
```

Pin to a specific commit SHA for reproducible production builds. Bump
the pin in the consuming service's `pyproject.toml` whenever you want
a new revision to ship.

For local dev, install the sibling working copy as editable:

```bash
pip install -e ../thecrag-py-runlog
```

(This overrides any SHA-pinned install, the same dual-mode pattern
used for `thecrag-py-api-client`.)

## Usage

```python
from datetime import datetime, timezone
from pathlib import Path
from thecrag_runlog import Recorder, RunRecord, touch_heartbeat

recorder = Recorder(
    service_name="my-service",
    output_path=Path("/data/status/status.json"),
    max_runs_per_task=20,
)

# At the end of each task run (cron tick, poll iteration, batch handler),
# record a RunRecord — the JSON file is rewritten atomically.
started = datetime.now(timezone.utc)
try:
    # ... do work ...
    processed, failed, error = 42, 0, None
finally:
    recorder.record(RunRecord(
        task_name="my-task",
        started_at=started,
        finished_at=datetime.now(timezone.utc),
        processed=processed,
        failed=failed,
        error=error,
    ))

# Schedule touch_heartbeat() at least once per minute (e.g. as an APScheduler
# IntervalTrigger job) so the Docker healthcheck sees the service as alive.
touch_heartbeat()
```

## Dockerfile integration

```dockerfile
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD ["python", "-m", "thecrag_runlog.healthcheck"]
```

## Environment variables

Both `touch_heartbeat()` and `python -m thecrag_runlog.healthcheck` read
these so the writer and checker always agree:

| Variable | Default | Purpose |
|---|---|---|
| `HEARTBEAT_PATH` | `/tmp/heartbeat` | Where the heartbeat file lives |
| `HEARTBEAT_MAX_AGE_SECONDS` | `300` | How stale the heartbeat is allowed to be before the healthcheck fails |

Override `HEARTBEAT_PATH` per service so multiple services on the same
host don't share a heartbeat file (e.g. `HEARTBEAT_PATH=/tmp/altitude-task.heartbeat`).

## License

[MIT](LICENSE) © 2026 theCrag. Permissive — use anywhere, modify freely, no attribution required beyond the unchanged license notice.
