"""Fire-and-forget analytics events for theCrag services.

``track_event()`` hands an event to a background thread and returns
immediately. The thread batches events and POSTs them to the fleet's
analytics collector, which buffers them again and pushes to GA4 on its own
schedule.

Three properties this module guarantees, in order of importance:

1. **It never blocks the caller.** ``track_event()`` is a bounded
   ``queue.put_nowait`` and nothing else. A collector that is down, slow or
   wedged costs a caller nothing — the property a synchronous POST cannot
   offer, since that would block every caller for its timeout.
2. **It never raises.** Same discipline as ``touch_heartbeat`` and
   ``Recorder.record``: analytics failures are logged, never propagated.
   Nothing about a service's real work should depend on telemetry working.
3. **It is inert unless configured.** With no ``ANALYTICS_URL`` set,
   ``track_event()`` returns immediately and no thread is ever started, so
   test suites and local dev emit nothing by default.

Stdlib only, deliberately — this package advertises zero runtime
dependencies, and one JSON POST does not justify imposing ``requests`` on
every service in the fleet.

Env vars, read at import time (the ``load_dotenv()`` ordering note in
``heartbeat`` applies here too):

===========================  ==========================================
``ANALYTICS_URL``            Collector endpoint. Unset disables entirely.
                             Fleet services should use the in-network
                             address (``http://analytics:8000/collect``)
                             rather than the public hostname.
``ANALYTICS_SERVICE``        This service's name; required once the URL
                             is set. Lowercase, ``[a-z0-9-]`` only.
``ANALYTICS_QUEUE_MAX``      Bounded queue size (default 10000).
``ANALYTICS_BATCH_SIZE``     Send once this many are queued (default 50).
                             Doubles as the per-POST cap.
``ANALYTICS_FLUSH_SECONDS``  Send anyway after this long (default 5).
``ANALYTICS_TIMEOUT``        Per-request timeout in seconds (default 5).
===========================  ==========================================
"""

from __future__ import annotations

import os
import re
import json
import time
import queue
import atexit
import logging
import threading
import urllib.error
import urllib.request
from typing import Any, Optional

logger = logging.getLogger(__name__)

ANALYTICS_URL = os.getenv("ANALYTICS_URL", "").strip()
ANALYTICS_SERVICE = os.getenv("ANALYTICS_SERVICE", "").strip()
ANALYTICS_QUEUE_MAX = int(os.getenv("ANALYTICS_QUEUE_MAX", "10000"))
ANALYTICS_BATCH_SIZE = int(os.getenv("ANALYTICS_BATCH_SIZE", "50"))
ANALYTICS_FLUSH_SECONDS = float(os.getenv("ANALYTICS_FLUSH_SECONDS", "5"))
ANALYTICS_TIMEOUT = float(os.getenv("ANALYTICS_TIMEOUT", "5"))

# Mirrors the collector's own rule. Enforced here too so a bad name fails in
# the logs of the service that owns it, rather than being silently dropped at
# the far end of a fire-and-forget call.
_SERVICE_NAME_RE = re.compile(r"^[a-z0-9-]+$")

# GA4 caps event names at 40 chars, alphanumeric + underscore, starting with a
# letter. Checked here so the mistake surfaces in the caller's own logs; the
# collector validates again and remains the authority.
_EVENT_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,39}$")

# Pushed onto the queue to wake the drain loop when a flush is requested. A
# distinct object rather than a falsy value, so it can never be mistaken for
# an event.
_WAKE = object()


class _Sender:
    """Owns the queue, the background thread and the drop counter."""

    def __init__(self, url: str, service: str) -> None:
        self.url = url
        self.service = service
        self._queue: queue.Queue = queue.Queue(maxsize=ANALYTICS_QUEUE_MAX)
        self._flush_requested = threading.Event()
        self._flush_done = threading.Event()
        self._lock = threading.Lock()
        self._dropped = 0
        self._thread = threading.Thread(
            target=self._run, name="thecrag-analytics", daemon=True
        )
        self._thread.start()

    # -- producer side (caller's thread) ---------------------------------

    def submit(self, event: dict) -> None:
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            # Drop rather than block. Analytics must never apply backpressure
            # to real work, and an unbounded queue is an OOM waiting for a
            # collector outage. Counted here, reported once per send.
            with self._lock:
                self._dropped += 1

    # -- consumer side (background thread) -------------------------------

    def _run(self) -> None:
        """Drain loop: send on batch-full or on deadline, whichever first.

        The queue and the timer are one mechanism — the "timer" is the
        ``timeout=`` on ``get()``. Time alone would let a burst accumulate
        into one enormous POST; count alone would leave a low-volume service
        sitting on events indefinitely and losing them on restart.
        """
        batch: list = []
        deadline = time.monotonic() + ANALYTICS_FLUSH_SECONDS

        while True:
            # This loop must never die. If it does, the service keeps calling
            # track_event() into a queue nobody drains and analytics is
            # silently gone for the rest of the process lifetime — with
            # nothing to indicate it. _send() swallows its own errors; this
            # is the backstop for everything else.
            try:
                try:
                    item = self._queue.get(timeout=max(0.0, deadline - time.monotonic()))
                except queue.Empty:
                    item = None

                if item is _WAKE:
                    item = None  # a wake-up token, not an event

                if item is not None:
                    batch.append(item)

                # An explicit flush_events() call takes everything queued, not
                # just what happens to be in `batch` — otherwise a caller that
                # flushes right after emitting would leave events behind.
                flushing = self._flush_requested.is_set()
                if flushing:
                    batch.extend(self._drain_remaining())

                if (
                    flushing
                    or len(batch) >= ANALYTICS_BATCH_SIZE
                    or time.monotonic() >= deadline
                ):
                    self._send(batch)
                    batch = []
                    deadline = time.monotonic() + ANALYTICS_FLUSH_SECONDS
                    if flushing:
                        self._flush_requested.clear()
                        self._flush_done.set()
            except Exception as e:  # pragma: no cover - defensive
                logger.warning("Analytics drain loop error, continuing: %s", e)
                batch = []
                deadline = time.monotonic() + ANALYTICS_FLUSH_SECONDS
                # Never leave a caller blocked on a flush that raised.
                self._flush_requested.clear()
                self._flush_done.set()

    def _drain_remaining(self) -> list:
        """Everything still queued, wake-up tokens excluded."""
        drained = []
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                return drained
            if item is not _WAKE:
                drained.append(item)

    def _send(self, batch: list) -> None:
        """Send a batch. Never raises — see the note in ``_run``."""
        try:
            dropped = self._take_dropped()
            if dropped:
                logger.warning("Analytics dropped %d event(s) — queue was full", dropped)

            if not batch:
                return

            # ANALYTICS_BATCH_SIZE doubles as the per-POST cap, so a backlog
            # goes out as several requests rather than one unwieldy payload.
            for start in range(0, len(batch), ANALYTICS_BATCH_SIZE):
                chunk = batch[start:start + ANALYTICS_BATCH_SIZE]
                self._post({"service": self.service, "events": chunk})
        except Exception as e:
            logger.warning("Analytics send failed for %d event(s): %s", len(batch), e)

    def _take_dropped(self) -> int:
        with self._lock:
            dropped, self._dropped = self._dropped, 0
            return dropped

    def _post(self, payload: dict) -> None:
        request = urllib.request.Request(
            self.url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=ANALYTICS_TIMEOUT):
                pass
        except Exception as e:
            # Never propagate. The caller is long gone, and telemetry must not
            # be able to disturb the service that emitted it.
            logger.warning(
                "Analytics send failed (%d event(s)): %s", len(payload["events"]), e
            )

    # -- flush ------------------------------------------------------------

    def flush(self, timeout: float) -> None:
        """Send everything queued now. The sender keeps running afterwards."""
        if not self._thread.is_alive():
            return

        self._flush_done.clear()
        self._flush_requested.set()
        try:
            self._queue.put_nowait(_WAKE)
        except queue.Full:
            # Saturated queue: the loop is busy draining anyway and will see
            # the flush request on its next pass.
            pass

        if not self._flush_done.wait(timeout):
            logger.warning("Analytics flush did not complete within %.1fs", timeout)


def _init() -> Optional[_Sender]:
    """Build the sender, or return None when analytics is not configured."""
    if not ANALYTICS_URL:
        return None

    if not ANALYTICS_SERVICE:
        logger.warning(
            "ANALYTICS_URL is set but ANALYTICS_SERVICE is not — analytics disabled. "
            "Every event needs a service name to be attributed in GA4."
        )
        return None

    if not _SERVICE_NAME_RE.match(ANALYTICS_SERVICE):
        logger.warning(
            "ANALYTICS_SERVICE %r is not lowercase [a-z0-9-] — analytics disabled. "
            "Name variants would each become a separate phantom user in GA4.",
            ANALYTICS_SERVICE,
        )
        return None

    return _Sender(ANALYTICS_URL, ANALYTICS_SERVICE)


_sender: Optional[_Sender] = _init()


def track_event(name: str, **params: Any) -> None:
    """Record one analytics event. Returns immediately; never raises.

    A no-op when ``ANALYTICS_URL`` is unset, which is the default.

    Args:
        name: GA4 event name — ``[A-Za-z][A-Za-z0-9_]*``, max 40 chars.
            Prefer a ``<domain>_<action>`` convention, e.g.
            ``pdf_guide_requested``.
        **params: Event parameters. GA4 allows at most 25 per event, with
            names up to 40 chars and values up to 100.

    Example:
        >>> track_event("pdf_guide_requested", area_id=123, single=0)
    """
    if _sender is None:
        return

    try:
        if not _EVENT_NAME_RE.match(name):
            logger.warning(
                "Ignoring analytics event %r: GA4 names must match "
                "[A-Za-z][A-Za-z0-9_]* and be at most 40 characters.",
                name,
            )
            return
        _sender.submit({"name": name, "params": dict(params)})
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("track_event(%r) failed: %s", name, e)


def flush_events(timeout: float = 5.0) -> None:
    """Send everything currently queued, then carry on.

    Safe to call at any time and any number of times — it does **not** shut
    the client down, so ``track_event()`` keeps working afterwards. Normally
    you only need it on a service's shutdown path; it is also registered with
    ``atexit``, but an explicit call is better because it runs while the
    service is still healthy rather than during interpreter teardown.

    Args:
        timeout: Seconds to wait for the in-flight send to complete.
    """
    if _sender is None:
        return
    try:
        _sender.flush(timeout)
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("flush_events() failed: %s", e)


if _sender is not None:
    atexit.register(flush_events)
