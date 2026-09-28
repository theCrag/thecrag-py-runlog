"""Tests for the fire-and-forget analytics client.

The module reads its config at import time and builds a module-level sender,
so most tests reload it under a patched environment via `analytics_module()`.
"""

import time
import importlib
import threading
from unittest.mock import patch

import pytest

import thecrag_runlog.analytics as analytics_mod


def analytics_module(monkeypatch, **env):
    """Re-import the module with the given environment."""
    for key in (
        "ANALYTICS_URL",
        "ANALYTICS_SERVICE",
        "ANALYTICS_QUEUE_MAX",
        "ANALYTICS_BATCH_SIZE",
        "ANALYTICS_FLUSH_SECONDS",
        "ANALYTICS_TIMEOUT",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))
    return importlib.reload(analytics_mod)


@pytest.fixture
def configured(monkeypatch):
    """A module with analytics on, its network call captured not performed."""
    mod = analytics_module(
        monkeypatch,
        ANALYTICS_URL="http://analytics:8000/collect",
        ANALYTICS_SERVICE="pdf-guide",
        ANALYTICS_BATCH_SIZE=3,
        ANALYTICS_FLUSH_SECONDS=0.2,
    )
    sent = []
    monkeypatch.setattr(mod._sender, "_post", lambda payload: sent.append(payload))
    yield mod, sent
    mod.flush_events(timeout=2)


# --- disabled by default -------------------------------------------------


def test_unconfigured_is_a_complete_no_op(monkeypatch):
    """The property that makes the runlog SHA bump safe for every service.

    No URL means no thread, no I/O, and no behaviour change anywhere — which
    is what lets existing services and test suites adopt this untouched.
    """
    before = threading.active_count()
    mod = analytics_module(monkeypatch)

    assert mod._sender is None
    mod.track_event("anything_at_all", foo=1)
    mod.flush_events()

    assert threading.active_count() == before


def test_url_without_service_is_disabled(monkeypatch):
    """A service name is mandatory — without it events can't be attributed."""
    mod = analytics_module(monkeypatch, ANALYTICS_URL="http://analytics:8000/collect")
    assert mod._sender is None


@pytest.mark.parametrize("bad", ["PDF-Guide", "pdf_guide", "pdf guide", "pdf.guide"])
def test_malformed_service_name_is_disabled(monkeypatch, bad):
    """Each variant would become its own phantom GA4 user — refuse to start."""
    mod = analytics_module(
        monkeypatch, ANALYTICS_URL="http://analytics:8000/collect", ANALYTICS_SERVICE=bad
    )
    assert mod._sender is None


# --- never blocks, never raises -----------------------------------------


def test_track_event_does_not_block_on_a_dead_collector(monkeypatch):
    """The core promise. A wedged collector must cost the caller nothing."""
    mod = analytics_module(
        monkeypatch,
        # Reserved TEST-NET-1 address; connections hang rather than refuse.
        ANALYTICS_URL="http://192.0.2.1:9/collect",
        ANALYTICS_SERVICE="pdf-guide",
        ANALYTICS_TIMEOUT=30,
    )

    start = time.monotonic()
    for i in range(1000):
        mod.track_event("some_event", i=i)
    elapsed = time.monotonic() - start

    assert elapsed < 1.0, f"track_event blocked for {elapsed:.2f}s"


def test_send_failures_never_reach_the_caller(configured):
    mod, _ = configured
    with patch.object(mod._sender, "_post", side_effect=OSError("connection refused")):
        for i in range(5):
            mod.track_event("some_event", i=i)
        time.sleep(0.5)  # let the drain loop hit the failure


def test_drain_thread_survives_a_failing_send(monkeypatch):
    """Regression: a raising _post used to kill the thread outright.

    The queue would then fill silently and analytics would be gone for the
    rest of the process lifetime, with nothing to indicate it — the exact
    silent failure this design exists to avoid.
    """
    mod = analytics_module(
        monkeypatch,
        ANALYTICS_URL="http://analytics:8000/collect",
        ANALYTICS_SERVICE="pdf-guide",
        ANALYTICS_BATCH_SIZE=1,
        ANALYTICS_FLUSH_SECONDS=0.1,
    )

    sent = []
    calls = {"n": 0}

    def flaky(payload):
        calls["n"] += 1
        if calls["n"] <= 3:
            raise OSError("collector down")
        sent.append(payload)

    monkeypatch.setattr(mod._sender, "_post", flaky)

    for i in range(3):
        mod.track_event("during_outage", i=i)
    time.sleep(0.6)

    assert mod._sender._thread.is_alive(), "drain thread died on a send failure"

    # And it must still deliver once the collector recovers.
    mod.track_event("after_recovery")
    mod.flush_events(timeout=2)

    assert any(
        e["name"] == "after_recovery" for p in sent for e in p["events"]
    ), "events were lost after the outage cleared"


def test_bad_event_name_is_ignored_not_raised(configured):
    mod, sent = configured

    mod.track_event("9_starts_with_digit")
    mod.track_event("has spaces")
    mod.track_event("x" * 41)
    mod.flush_events(timeout=2)

    assert [e for p in sent for e in p["events"]] == []


# --- batching ------------------------------------------------------------


def test_full_batch_sends_without_waiting_for_the_timer(configured):
    """Count trigger: 3 events must go out well before the 0.2s deadline."""
    mod, sent = configured

    start = time.monotonic()
    for i in range(3):
        mod.track_event("some_event", i=i)

    deadline = time.monotonic() + 2
    while not sent and time.monotonic() < deadline:
        time.sleep(0.01)

    assert sent, "batch-full did not trigger a send"
    assert len(sent[0]["events"]) == 3
    assert time.monotonic() - start < 0.2


def test_single_event_still_sends_on_the_timer(configured):
    """Time trigger: one event must not sit queued forever."""
    mod, sent = configured
    mod.track_event("lonely_event")

    deadline = time.monotonic() + 2
    while not sent and time.monotonic() < deadline:
        time.sleep(0.01)

    assert sent
    assert len(sent[0]["events"]) == 1


def test_backlog_is_chunked_to_the_batch_cap(configured):
    """No single POST may exceed the cap, however large the backlog."""
    mod, sent = configured
    for i in range(10):
        mod.track_event("some_event", i=i)
    mod.flush_events(timeout=2)

    assert all(len(p["events"]) <= 3 for p in sent)
    assert sum(len(p["events"]) for p in sent) == 10


def test_payload_carries_the_service_name(configured):
    mod, sent = configured
    mod.track_event("some_event", area_id=123)
    mod.flush_events(timeout=2)

    assert sent[0]["service"] == "pdf-guide"
    assert sent[0]["events"][0] == {"name": "some_event", "params": {"area_id": 123}}


# --- bounded queue -------------------------------------------------------


def test_queue_drops_when_full_rather_than_growing(monkeypatch):
    """Analytics must never OOM a service or apply backpressure to real work."""
    mod = analytics_module(
        monkeypatch,
        ANALYTICS_URL="http://analytics:8000/collect",
        ANALYTICS_SERVICE="pdf-guide",
        ANALYTICS_QUEUE_MAX=10,
        ANALYTICS_BATCH_SIZE=1000,      # never fills, so nothing is drained
        ANALYTICS_FLUSH_SECONDS=3600,   # and the timer never fires
    )
    monkeypatch.setattr(mod._sender, "_post", lambda payload: None)

    for i in range(100):
        mod._sender.submit({"name": "e", "params": {"i": i}})

    assert mod._sender._queue.qsize() <= 10
    assert mod._sender._dropped >= 85


def test_drop_count_is_reported_and_reset(configured):
    mod, _ = configured
    with mod._sender._lock:
        mod._sender._dropped = 7

    assert mod._sender._take_dropped() == 7
    assert mod._sender._take_dropped() == 0


# --- shutdown ------------------------------------------------------------


def test_flush_drains_before_returning(configured):
    mod, sent = configured
    mod.track_event("one")
    mod.track_event("two")

    mod.flush_events(timeout=2)

    assert sum(len(p["events"]) for p in sent) == 2


def test_wake_token_is_never_sent_as_an_event(configured):
    """Regression: the flush wake-up token must not leak into the payload."""
    mod, sent = configured
    mod.track_event("real_event")
    mod.flush_events(timeout=2)

    for payload in sent:
        for event in payload["events"]:
            assert isinstance(event, dict) and "name" in event


def test_flush_does_not_shut_the_client_down(configured):
    """Regression: flush used to stop the sender permanently.

    The name says "flush", so a caller reasonably expects "send now" — but it
    also ended the background thread, and every subsequent track_event() was
    silently discarded. Found by an end-to-end smoke test whose final event
    vanished.
    """
    mod, sent = configured

    mod.track_event("before_flush")
    mod.flush_events(timeout=2)

    mod.track_event("after_flush")
    mod.flush_events(timeout=2)

    names = [e["name"] for p in sent for e in p["events"]]
    assert "before_flush" in names
    assert "after_flush" in names, "events after a flush were dropped"


def test_flush_is_idempotent(configured):
    """atexit may fire after an explicit call — that must be harmless."""
    mod, _ = configured
    mod.track_event("one")
    mod.flush_events(timeout=2)
    mod.flush_events(timeout=2)
    mod.flush_events(timeout=2)


def test_flush_sends_everything_queued_not_just_the_current_batch(configured):
    """A caller that flushes right after emitting must not leave events behind."""
    mod, sent = configured
    for i in range(7):  # batch size is 3, so this straddles several batches
        mod.track_event("some_event", i=i)

    mod.flush_events(timeout=2)

    assert sum(len(p["events"]) for p in sent) == 7


def teardown_module():
    """Leave the module in its default (disabled) state for other tests."""
    importlib.reload(analytics_mod)
