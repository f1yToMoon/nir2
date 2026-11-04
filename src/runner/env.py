"""Stand endpoints and resources of the runner (environment of the `runner` container)."""
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Env:
    routing: str = field(default_factory=lambda: _env("CATALOG_ROUTING", "proxy"))
    proxy_uri: str = field(default_factory=lambda: _env("CAPL_PROXY_URI", "http://capl:8180"))
    advisor_uri: str = field(default_factory=lambda: _env("CAPL_ADVISOR_URI", "http://capl:8190"))
    direct_uri: str = field(default_factory=lambda: _env("CATALOG_DIRECT_URI", "http://catalog:8181"))
    warehouse: str = field(default_factory=lambda: _env("WAREHOUSE", "s3://warehouse/"))
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://toxiproxy:18333"))
    toxiproxy_api: str = field(default_factory=lambda: _env("TOXIPROXY_API", "http://toxiproxy:8474"))
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER", "spark://spark-master:7077"))
    driver_memory: str = field(default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "3g"))
    executor_memory: str = field(default_factory=lambda: _env("SPARK_EXECUTOR_MEMORY", "2g"))
    executor_cores: str = field(default_factory=lambda: _env("SPARK_EXECUTOR_CORES", "2"))
    cores_max: str = field(default_factory=lambda: _env("SPARK_CORES_MAX", "8"))
    kafka: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "kafka:9092"))
    flink_gateway: str = field(default_factory=lambda: _env("FLINK_SQL_GATEWAY",
                                                            "http://flink-sql-gateway:8083"))
    flink_rest: str = field(default_factory=lambda: _env("FLINK_REST", "http://flink-jobmanager:8081"))
    pg_dsn: str = field(default_factory=lambda: _env(
        "TELEMETRY_PG_DSN", "postgresql://capl:capl@postgres:5432/telemetry"))
    pg_schema: str = field(default_factory=lambda: _env("TELEMETRY_SCHEMA", "stand"))
    catalog_impl: str = field(default_factory=lambda: _env("CATALOG_IMPL", "rest_fixture"))
    iceberg_version: str = field(default_factory=lambda: _env("ICEBERG_VERSION", "1.12.0"))
    spark_ui_port: int = 4040

    @property
    def catalog_uri(self) -> str:
        """URI of the catalog for all Iceberg clients of a run (proxy | bypass, AC-5)."""
        return self.proxy_uri if self.routing == "proxy" else self.direct_uri


def git_sha() -> str:
    """Commit of the code; `-dirty` if the tree has changes (GIT_SHA from make wins)."""
    if os.environ.get("GIT_SHA"):
        return os.environ["GIT_SHA"]
    try:
        sha = subprocess.run(["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True,
                             text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "-C", str(REPO), "status", "--porcelain"],
                               capture_output=True, text=True, check=True).stdout.strip()
        return sha + ("-dirty" if dirty else "")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
