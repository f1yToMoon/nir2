"""runner.scenario, runner.intents: loader, variants, levels, determinism (tz/06 steps 3.1, NFR-5)."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from runner import scenario as scn
from runner.intents import bucket_long, fingerprint, generate, murmur3_long, partition_str
from runner.ops.executor import fp_predicate

ALL = ["S0_baseline", "S1_compaction_vs_stream_append", "S2_compaction_vs_cdc_merge",
       "S3_compaction_vs_mor_dml", "S4_merge_vs_stream_serializable", "S5_compliance_delete",
       "S6_mixed_drift"]


def test_validate_config_cli_passes() -> None:
    root = Path(__file__).parents[2]
    assert scn.main(["--validate", str(root / "config/workloads"),
                     str(root / "config/experiments/matrix.yaml")]) == 0


def test_extends_merges_and_overrides() -> None:
    sc = scn.load("S3_compaction_vs_mor_dml")
    assert sc.table.properties["write.update.mode"] == "merge-on-read"      # from S3
    assert sc.table.properties["commit.retry.num-retries"] == "4"            # from _base
    assert sc.expire is not None and sc.expire.older_than_s == 540  # ADR-012: measured retention window


def test_variant_patches_ops_by_id_and_v3_drops_rewrite_deletes() -> None:
    v2 = scn.load("S3_compaction_vs_mor_dml")
    v3 = scn.load("S3_compaction_vs_mor_dml", variant="v3_deletion_vectors")
    assert v3.table.format_version == 3
    assert {o.id for o in v2.active_ops} - {o.id for o in v3.active_ops} == {"delete_rewrite"}
    s1 = scn.load("S1_compaction_vs_stream_append", variant="rewrite_partial_progress")
    assert s1.controlled_ops[0].params["partial_progress"] is True
    assert s1.controlled_ops[0].params["min_input_files"] == 5             # rest of the op kept


def test_variant_with_unknown_op_id_fails(tmp_path: Path) -> None:
    root = Path(__file__).parents[2] / "config/workloads"
    raw = yaml.safe_load((root / "S1_compaction_vs_stream_append.yaml").read_text())
    raw["extends"] = str(root / "_base.yaml")
    raw["variants"]["bad"] = {"controlled_ops": [{"id": "typo", "params": {}}]}
    p = tmp_path / "S9_bad.yaml"
    p.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="unknown controlled op"):
        scn.load(str(p), variant="bad", workloads=tmp_path)


def test_level_scaling() -> None:
    mid = scn.load("S2_compaction_vs_cdc_merge", level="mid")
    high = scn.load("S2_compaction_vs_cdc_merge", level="high")
    low = scn.load("S2_compaction_vs_cdc_merge", level="low")
    assert high.controlled_ops[1].arrival.interval_s == mid.controlled_ops[1].arrival.interval_s / 2
    assert high.controlled_ops[1].max_concurrent == 4
    assert low.controlled_ops[1].max_concurrent == 1                       # ceil(2 * 0.5)
    assert high.streams[0].rate_events_per_s == 1.5 * mid.streams[0].rate_events_per_s
    assert high.streams[0].commit_period_s == mid.streams[0].commit_period_s  # S2-R3-06


def test_overwrite_without_validate_from_is_rejected() -> None:
    with pytest.raises(ValidationError, match="validate_from_snapshot_id"):
        scn.ControlledOp.model_validate({
            "id": "o", "type": "overwrite", "arrival": {"process": "fixed", "interval_s": 10},
            "max_concurrent": 1, "sla_s": 10, "footprint": {"selector": "single_day", "min_age_days": 1},
            "params": {"isolation_level_option": "serializable"}})


def test_lite_profile() -> None:
    sc = scn.load("S2_compaction_vs_cdc_merge", size="lite")
    assert sc.seed_data.rows_total == 9_000_000 and sc.seed_data.files_per_partition == 20
    assert all(s.engine == "spark" for s in sc.streams)


@pytest.mark.parametrize("name", ALL)
def test_same_seed_same_intents(name: str) -> None:
    sc = scn.load(name, level="mid")
    a, b = generate(sc, 11, "r1"), generate(sc, 11, "r2")
    assert a and fingerprint(a) == fingerprint(b)
    c = generate(sc, 12, "r1")
    assert fingerprint(c) != fingerprint(a)          # the check can fail: the seed matters


def test_intents_inside_measured_window() -> None:
    sc = scn.load("S2_compaction_vs_cdc_merge", level="mid")
    it = generate(sc, 1, "r")
    assert min(i.t_offset_s for i in it) >= sc.warmup_s
    assert max(i.t_offset_s for i in it) < sc.warmup_s + sc.duration_s
    assert len({i.op_id for i in it}) == len(it)


def test_seeded_footprints() -> None:
    sc = scn.load("S5_compliance_delete", level="mid")
    gd = next(i for i in generate(sc, 3, "r") if i.op_key == "gdpr_delete")
    assert len(gd.footprint["keys"]) == 500 and len(gd.footprint["buckets"]) == 2
    assert all(bucket_long(k, 8) in gd.footprint["buckets"] for k in gd.footprint["keys"])
    assert len(gd.footprint["partitions"]) == 30 * 2
    mg = next(i for i in generate(sc, 3, "r") if i.op_key == "cdc_merge")
    assert 2 <= len(mg.footprint["partitions"]) <= 6


def test_murmur3_spec_vector() -> None:
    # Iceberg spec, Appendix B: hash(34L) = 2017239379
    assert murmur3_long(34) == 2017239379
    assert 0 <= bucket_long(-1, 8) < 8


def test_fp_predicate_groups_by_bucket() -> None:
    from datetime import date
    parts = [partition_str(date(2026, 11, 30), 3), partition_str(date(2026, 11, 29), 3),
             partition_str(date(2026, 11, 30), 5)]
    p = fp_predicate(parts, 8)
    assert p == ("((system.bucket(8, key) = 3 AND dt IN (DATE '2026-11-29', DATE '2026-11-30')) OR "
                 "(system.bucket(8, key) = 5 AND dt = DATE '2026-11-30'))")
