"""Read provenance of Spark 4.0 / Iceberg 1.12 row-level commands.

The command retains its physical write plan under CommandResult; AQE/query stages retain
the executed child plan. Iceberg pins snapshotId on SparkCopyOnWriteScan and
SparkBatchQueryScan (SparkScanBuilder.java:655-729). These package-private accessors are
read through reflection. No SQL, client code, table state, or scan is changed.
"""
from __future__ import annotations

from typing import Any


def _accessor(obj: Any, name: str, gateway: Any) -> Any:
    cls = obj.getClass()
    while cls is not None:
        for method in cls.getDeclaredMethods():
            if method.getName() == name and method.getParameterCount() == 0:
                method.setAccessible(True)
                return method.invoke(obj, gateway.new_array(gateway.jvm.java.lang.Object, 0))
        cls = cls.getSuperclass()
    raise ValueError(f"missing Iceberg scan accessor {name}")


def read_snapshot_id(frame: Any, table: str) -> int:
    """Require one consistent target snapshot from the executed row-level scan(s).

    `table` includes the slot catalog: unrelated source scans must not define provenance.
    Missing/ambiguous provenance invalidates the ledger, rather than falling back to a
    planning snapshot or the successful commit's parent (which may include unread appends).
    """
    gateway = frame.sparkSession.sparkContext._gateway
    jvm = gateway.jvm
    pending = [frame._jdf.queryExecution().executedPlan()]
    seen: set[int] = set()
    snapshots: set[int] = set()
    while pending:
        node = pending.pop()
        identity = int(jvm.java.lang.System.identityHashCode(node))
        if identity in seen:
            continue
        seen.add(identity)
        name = str(node.getClass().getSimpleName())
        if name == "BatchScanExec":
            scan = node.scan()
            kind = str(scan.getClass().getSimpleName())
            if (kind in ("SparkCopyOnWriteScan", "SparkBatchQueryScan")
                    and str(_accessor(scan, "table", gateway).name()) == table):
                sid = _accessor(scan, "snapshotId", gateway)
                if sid is not None:
                    snapshots.add(int(sid))
        for seq in (node.children(), node.innerChildren()):
            it = seq.iterator()
            while it.hasNext():
                pending.append(it.next())
        if name == "AdaptiveSparkPlanExec":
            pending.append(node.executedPlan())
        elif name.endswith("QueryStageExec"):
            pending.append(node.plan())
    if len(snapshots) != 1:
        raise ValueError(f"expected one actual read snapshot for {table}, got {sorted(snapshots)}")
    return snapshots.pop()
