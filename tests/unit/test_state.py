"""capl.state: windowed intensities (tz/03 §3.1) and periodic-writer detection."""
from __future__ import annotations

import random

import pytest

from capl.config import StateConfig
from capl.state import StateStore


def _ev(t: float, sid: int, op: str = "append", writer: str | None = None,
        parts: tuple[str, ...] = ("dt=1",)) -> dict[str, object]:
    return {"table": "t", "committed_at": t, "snapshot_id": sid, "op_class": op, "writer_id": writer,
            "partitions": [{"partition": p} for p in parts]}


def test_window_rate_and_point_in_time() -> None:
    st = StateStore(StateConfig(intensity_windows_s=[60.0], intensity_weights=[1.0]))
    for i in range(30):
        st.add_commit_event(_ev(1000 + 2 * i, i))          # 1 commit / 2 s in [1000, 1058]
    assert st.window_rate("t", 60, now=1060) == pytest.approx(30 / 60)
    # the event at exactly t=1058 does not count at now=1058 (point-in-time, tz/03 §4.1)
    assert st.window_rate("t", 60, now=1058) == pytest.approx(29 / 60)
    assert st.window_rate("t", 10, now=1060) == pytest.approx(5 / 10)
    assert st.intensity("t", now=1060, op_class="rewrite_data") == 0.0
    assert st.intensity("t", now=1060, partition="dt=2") == 0.0


def test_intensity_mix_of_windows() -> None:
    st = StateStore(StateConfig(intensity_windows_s=[10.0, 100.0], intensity_weights=[0.5, 0.5]))
    for i in range(100):
        st.add_commit_event(_ev(float(i), i))
    assert st.intensity("t", now=100) == pytest.approx(0.5 * 1.0 + 0.5 * 1.0)


def test_periodic_writer_detected_and_predicts() -> None:
    st = StateStore(StateConfig())
    rng = random.Random(1)
    t = 5.0
    for i in range(20):
        st.add_commit_event(_ev(t, i, writer="flink-1", parts=("dt=0",) if i % 5 else ("dt=-1",)))
        t += 10 + rng.uniform(-0.5, 0.5)
        st.add_commit_event(_ev(t + rng.uniform(0, 30), 1000 + i, writer="adhoc"))
    pw = st.periodic_writers("t")
    assert set(pw) == {"flink-1"}                      # irregular writer rejected by CV
    w = pw["flink-1"]
    assert w.period_s == pytest.approx(10, abs=0.5)
    assert w.partitions["dt=0"] == pytest.approx(0.8)
    assert w.expected_commits(1000.0, 1100.0) in (9, 10, 11)


def test_ring_buffer_bound() -> None:
    st = StateStore(StateConfig(ring_buffer_per_table=10))
    for i in range(100):
        st.add_commit_event(_ev(float(i), i))
    assert len(st.tables["t"].commits) == 10
