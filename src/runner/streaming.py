"""Uncontrolled writers of a run (FR-RUN-2, tz/06 step 3.6).

* `KafkaFeed` — seeded producer of a stream into a per-run topic, with the drift of S6 and the
  stream_rate_mult of the level; counts produced events per partition (ledger, NFR-4).
* `FlinkStream` — the Flink SQL job Kafka → Iceberg via the SQL Gateway (template
  infra/flink/sql/streaming_<mode>.sql), stopped after the consumer group has no lag and the
  last checkpoint has committed (exactly-once: every produced event is in the table).
* `SparkAgents` — light Spark append writers (S1 «агенты», lite profile): every
  `commit_period_s` each writer appends its share of `rate_events_per_s` of the stream through its
  own slot catalog `a<k>`; rows of failed appends are not counted.
"""
from __future__ import annotations

import contextlib
import json
import re
import threading
import time
from array import array
from collections import Counter
from datetime import date
from typing import Any

import requests

from runner.datagen import EventGen, partition_of
from runner.env import REPO, Env
from runner.intents import partition_str
from runner.scenario import Scenario, Stream
from runner.slots import job_scope

FLINK_SQL_DIR = REPO / "infra" / "flink" / "sql"


class EventLog:
    """Compact per-event log (id, key, partition) of rows that must reach the table."""

    def __init__(self) -> None:
        self.ids = array("q")
        self.keys = array("q")
        self.part_idx = array("H")
        self.generated_at = array("d")
        self.committed_at = array("d")
        self.positions: dict[int, int] = {}
        self.lock = threading.RLock()
        self.labels: list[str] = []
        self._index: dict[str, int] = {}

    def __getstate__(self) -> dict[str, Any]:
        with self.lock:
            state = dict(self.__dict__)
            state.pop("lock")
            state.pop("positions")
            return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self.lock = threading.RLock()
        self.positions = {event_id: i for i, event_id in enumerate(self.ids)}

    def add(self, event_id: int, key: int, part: str, generated_at: float | None = None) -> None:
        with self.lock:
            self._add(event_id, key, part, generated_at)

    def _add(self, event_id: int, key: int, part: str, generated_at: float | None) -> None:
        i = self._index.get(part)
        if i is None:
            i = self._index[part] = len(self.labels)
            self.labels.append(part)
        self.positions[event_id] = len(self.ids)
        self.generated_at.append(time.time() if generated_at is None else generated_at)
        self.committed_at.append(float("nan"))
        self.ids.append(event_id)
        self.keys.append(key)
        self.part_idx.append(i)


class KafkaFeed:
    def __init__(self, env: Env, sc: Scenario, stream: Stream, stream_idx: int, run_id: str,
                 seed: int, t0_mono: float) -> None:
        self.env, self.sc, self.stream = env, sc, stream
        # attempt stamp: a topic left by an aborted attempt of the same run id (same seed → same
        # event ids, earliest-offset) must never be read again (stage-3 S0 run: 1 277 600 duplicates)
        self.topic = f"capl-{run_id}-{stream.id}-{int(time.time())}".replace("_", "-")
        self.gen = EventGen(sc, stream, stream_idx, None, seed)
        self.first_id = self.gen.next_id
        self.t0 = t0_mono
        self.produced: Counter[str] = Counter()
        self.n = 0
        self._stop = threading.Event()
        self._thr: threading.Thread | None = None
        self.errors: list[str] = []
        self.events = EventLog()

    def create_topic(self, partitions: int = 8) -> None:
        from confluent_kafka.admin import AdminClient, NewTopic
        adm = AdminClient({"bootstrap.servers": self.env.kafka})
        prefix = self.topic.rsplit("-", 1)[0]
        stale = [t for t in adm.list_topics(timeout=10).topics if t.startswith(prefix)]
        if stale:                                   # topics of aborted attempts of this run id
            for f in adm.delete_topics(stale).values():
                with contextlib.suppress(Exception):
                    f.result(timeout=30)
        fs = adm.create_topics([NewTopic(self.topic, num_partitions=partitions, replication_factor=1)])
        for f in fs.values():
            with contextlib.suppress(Exception):
                f.result(timeout=30)

    def delete_topic(self) -> None:
        from confluent_kafka.admin import AdminClient
        # delete_topics is asynchronous: wait for the futures, else the process may exit first
        # (topics of the first stage-3 runs were left in Kafka)
        with contextlib.suppress(Exception):
            for f in AdminClient({"bootstrap.servers": self.env.kafka}).delete_topics([self.topic]).values():
                f.result(timeout=30)

    def _rate(self) -> float:
        from runner.intents import drift_factor
        return self.stream.rate_events_per_s * drift_factor(self.sc, time.monotonic() - self.t0,
                                                            "streams")

    def _loop(self) -> None:
        from confluent_kafka import Producer
        p = Producer({"bootstrap.servers": self.env.kafka, "linger.ms": 20,
                      "enable.idempotence": True, "acks": "all"})
        nb = self.sc.table.n_buckets
        tick = 0.1
        owed = 0.0
        nxt = time.monotonic()
        while not self._stop.is_set():
            owed += self._rate() * tick
            k = int(owed)
            owed -= k
            if k:
                for ev in self.gen.batch(k):
                    generated_at = time.time()
                    while True:
                        try:
                            p.produce(self.topic, json.dumps(ev).encode(),
                                      key=str(ev["key"]).encode())
                            break
                        except BufferError:
                            p.poll(0.05)
                    d, b = partition_of(ev, nb)
                    part = partition_str(date.fromisoformat(d), b)
                    self.produced[part] += 1
                    self.events.add(ev["event_id"], ev["key"], part, generated_at)
                    self.n += 1
            p.poll(0)
            nxt += tick
            self._stop.wait(max(0.0, nxt - time.monotonic()))
        left = p.flush(60)
        if left:
            self.errors.append(f"{left} messages not delivered")

    def start(self) -> None:
        self._thr = threading.Thread(target=self._loop, name=f"kafka-{self.stream.id}", daemon=True)
        self._thr.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thr:
            self._thr.join(timeout=120)

    @property
    def id_range(self) -> tuple[int, int]:
        return self.first_id, self.gen.next_id

    def lag(self, group: str) -> int | None:
        """Total lag of the consumer group (None if offsets are not committed yet)."""
        from confluent_kafka import Consumer, TopicPartition
        c = Consumer({"bootstrap.servers": self.env.kafka, "group.id": group,
                      "enable.auto.commit": False})
        try:
            md = c.list_topics(self.topic, timeout=10)
            tps = [TopicPartition(self.topic, p) for p in md.topics[self.topic].partitions]
            committed = c.committed(tps, timeout=10)
            total = 0
            for tp in committed:
                lo, hi = c.get_watermark_offsets(TopicPartition(self.topic, tp.partition), timeout=10)
                if tp.offset < 0:
                    if hi > lo:
                        return None
                    continue
                total += hi - tp.offset
            return total
        finally:
            c.close()


def _render(template: str, values: dict[str, Any]) -> list[str]:
    text = template
    for k, v in values.items():
        text = text.replace("{{" + k + "}}", str(v))
    lines = [re.sub(r"\s--.*$", "", ln) for ln in text.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


class FlinkStream:
    def __init__(self, env: Env, sc: Scenario, stream: Stream, run_id: str, feed: KafkaFeed,
                 parallelism: int = 2) -> None:
        self.env, self.sc, self.stream, self.run_id, self.feed = env, sc, stream, run_id, feed
        self.parallelism = parallelism
        self.group = f"capl-{stream.id}-{run_id}"
        self.sid: str | None = None
        self.job_id: str | None = None
        self.pipeline = f"capl-stream-{stream.id}-{run_id}"

    def _sql(self, stmt: str, wait: bool = True) -> str:
        gw = self.env.flink_gateway
        r = requests.post(f"{gw}/v1/sessions/{self.sid}/statements", json={"statement": stmt},
                          timeout=30)
        r.raise_for_status()
        op = str(r.json()["operationHandle"])
        if not wait:
            return op
        for _ in range(120):
            st = requests.get(f"{gw}/v1/sessions/{self.sid}/operations/{op}/status",
                              timeout=10).json().get("status")
            if st == "FINISHED":
                return op
            if st in ("ERROR", "CLOSED", "CANCELED"):
                res = requests.get(f"{gw}/v1/sessions/{self.sid}/operations/{op}/result/0",
                                   timeout=10).text[:1500]
                raise RuntimeError(f"Flink SQL {st}: {stmt[:200]} → {res}")
            time.sleep(0.5)
        raise TimeoutError(stmt[:200])

    def start(self) -> None:
        mode = "upsert" if self.stream.mode == "upsert" else "append"
        stmts = _render((FLINK_SQL_DIR / f"streaming_{mode}.sql").read_text(), {
            "commit_period_s": int(self.stream.commit_period_s), "stream_id": self.stream.id,
            "run_id": self.run_id, "parallelism": self.parallelism,
            "catalog_uri": self.env.catalog_uri, "warehouse": self.env.warehouse,
            "s3_endpoint": self.env.s3_endpoint, "topic": self.feed.topic,
            "kafka_bootstrap": self.env.kafka})
        gw = self.env.flink_gateway
        r = requests.post(f"{gw}/v1/sessions", json={"properties": {
            "execution.runtime-mode": "streaming",
            "state.checkpoints.dir": f"file:///flink-checkpoints/{self.run_id}"}}, timeout=10)
        r.raise_for_status()
        self.sid = r.json()["sessionHandle"]
        for s in stmts:
            if s.upper().startswith("SET "):
                m = re.match(r"SET\s+'([^']+)'\s*=\s*'([^']*)'", s, re.I)
                if m:
                    self._sql(f"SET '{m.group(1)}' = '{m.group(2)}'")
                    continue
            insert = s.upper().startswith("INSERT")
            self._sql(s, wait=not insert)
        for _ in range(60):
            time.sleep(2)
            for j in requests.get(f"{self.env.flink_rest}/jobs/overview", timeout=10).json().get(
                    "jobs", []):
                if j.get("name") == self.pipeline and j.get("state") == "RUNNING":
                    self.job_id = j["jid"]
                    return
        raise RuntimeError(f"Flink job {self.pipeline} not RUNNING within 120 s")

    def drain_and_stop(self, timeout_s: float = 300) -> dict[str, Any]:
        """After the producer stopped: wait for zero lag of the consumer group (offsets are
        committed on checkpoint completion), then two more checkpoint periods for the Iceberg
        commit of that checkpoint, then cancel."""
        t_end = time.monotonic() + timeout_s
        lag: int | None = None
        while time.monotonic() < t_end:
            lag = self.feed.lag(self.group)
            if lag == 0:
                break
            time.sleep(2)
        time.sleep(2 * self.stream.commit_period_s + 2)
        out = {"final_lag": lag, "job_id": self.job_id}
        out["checkpoints"] = self.checkpoints()
        self.cancel()
        return out

    def checkpoints(self) -> dict[str, Any]:
        if not self.job_id:
            return {}
        with contextlib.suppress(Exception):
            c = requests.get(f"{self.env.flink_rest}/jobs/{self.job_id}/checkpoints", timeout=10).json()
            return dict(c.get("counts", {}))
        return {}

    def restarts(self) -> int:
        if not self.job_id:
            return 0
        with contextlib.suppress(Exception):
            j = requests.get(f"{self.env.flink_rest}/jobs/{self.job_id}", timeout=10).json()
            return int(j.get("status-counts", {}).get("RESTARTING", 0))
        return 0

    def cancel(self) -> None:
        if self.job_id:
            with contextlib.suppress(Exception):
                requests.patch(f"{self.env.flink_rest}/jobs/{self.job_id}?mode=cancel", timeout=15)
        if self.sid:
            with contextlib.suppress(Exception):
                requests.delete(f"{self.env.flink_gateway}/v1/sessions/{self.sid}", timeout=10)


class SparkAgents:
    """`writers` threads appending `rate_events_per_s / writers` events every commit period."""

    def __init__(self, spark: Any, sc: Scenario, stream: Stream, stream_idx: int, run_id: str,
                 seed: int, t0_mono: float, slot_offset: int = 0) -> None:
        self.spark, self.sc, self.stream, self.run_id = spark, sc, stream, run_id
        self.gens = [EventGen(sc, stream, stream_idx, w, seed) for w in range(stream.writers)]
        self.first_ids = [g.next_id for g in self.gens]
        self.slot_offset = slot_offset
        self.t0 = t0_mono
        self.committed: Counter[str] = Counter()
        self.committed_ids: list[tuple[int, int]] = []     # [lo, hi) of successful appends
        self.failed_ids: list[tuple[int, int]] = []
        self.n_ok = 0
        self.n_fail = 0
        self.errors: list[str] = []
        self.events = EventLog()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def _schema(self) -> Any:
        from pyspark.sql.types import (
            DateType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampNTZType,
        )
        return StructType([
            StructField("event_id", LongType(), False), StructField("key", LongType(), False),
            StructField("dt", DateType(), False), StructField("event_ts", TimestampNTZType()),
            StructField("amount", DoubleType()), StructField("status", StringType()),
            StructField("version", IntegerType()), StructField("payload", StringType())])

    def _writer(self, w: int) -> None:
        from datetime import datetime as _dt

        from runner.intents import drift_factor
        g = self.gens[w]
        slot = f"a{self.slot_offset + w}"
        table = f"{slot}.{self.sc.table.name}"
        period = self.stream.commit_period_s
        schema = self._schema()
        nb = self.sc.table.n_buckets
        # writers start spread over the period
        self._stop.wait(period * w / max(1, len(self.gens)))
        n_commit = 0
        while not self._stop.is_set():
            t = time.monotonic()
            rate = self.stream.rate_events_per_s / len(self.gens) * drift_factor(
                self.sc, t - self.t0, "streams")
            n = max(1, round(rate * period))
            generated_at = time.time()
            evs = g.batch(n)
            lo, hi = evs[0]["event_id"], evs[-1]["event_id"] + 1
            rows = [(e["event_id"], e["key"], date.fromisoformat(e["dt"]),
                     _dt.fromisoformat(e["event_ts"]), e["amount"], e["status"], e["version"],
                     e["payload"]) for e in evs]
            tag = f"agent-{self.run_id}-{w}-{n_commit}"
            n_commit += 1
            try:
                with job_scope(self.spark, tag, f"agent {w}", "streaming"):
                    self.spark.createDataFrame(rows, schema).writeTo(table).append()
                with self._lock:
                    self.n_ok += 1
                    self.committed_ids.append((lo, hi))
                    for e in evs:
                        d, b = partition_of(e, nb)
                        part = partition_str(date.fromisoformat(d), b)
                        self.committed[part] += 1
                        self.events.add(e["event_id"], e["key"], part, generated_at)
            except Exception as exc:
                with self._lock:
                    self.n_fail += 1
                    self.failed_ids.append((lo, hi))
                    self.errors.append(str(exc)[:300])
            self._stop.wait(max(0.0, period - (time.monotonic() - t)))

    def start(self) -> None:
        from pyspark import InheritableThread
        for w in range(len(self.gens)):
            th = InheritableThread(target=self._writer, args=(w,), name=f"agent-{w}", daemon=True)
            th.start()
            self._threads.append(th)

    def stop(self) -> None:
        self._stop.set()
        for th in self._threads:
            th.join(timeout=300)
