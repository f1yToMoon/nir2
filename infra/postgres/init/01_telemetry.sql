-- Телеметрия CAPL и runner. Выполняется при первом старте контейнера postgres.
-- Также создаётся БД для Lakekeeper (профиль lakekeeper).

CREATE USER capl WITH PASSWORD 'capl';
CREATE DATABASE telemetry OWNER capl;
CREATE USER lakekeeper WITH PASSWORD 'lakekeeper';
CREATE DATABASE lakekeeper OWNER lakekeeper;

\connect telemetry

SET ROLE capl;

CREATE SCHEMA IF NOT EXISTS stand;   -- данные стенда
CREATE SCHEMA IF NOT EXISTS sim;     -- данные симулятора (та же структура)

DO $$
DECLARE s text;
BEGIN
  FOREACH s IN ARRAY ARRAY['stand', 'sim'] LOOP
    EXECUTE format($f$

      CREATE TABLE IF NOT EXISTS %1$I.runs (
        run_id          text PRIMARY KEY,
        scenario        text NOT NULL,
        policy          text NOT NULL,
        policy_params   jsonb NOT NULL DEFAULT '{}',
        concurrency     text,
        seed            integer NOT NULL,
        catalog_impl    text,
        iceberg_version text,
        git_sha         text,
        model_version   text,
        params          jsonb NOT NULL DEFAULT '{}',
        started_at      timestamptz NOT NULL,
        finished_at     timestamptz,
        status          text NOT NULL DEFAULT 'running',
        invariants_ok   boolean,
        notes           text
      );

      CREATE TABLE IF NOT EXISTS %1$I.ops (
        op_id               text PRIMARY KEY,
        run_id              text NOT NULL REFERENCES %1$I.runs(run_id) ON DELETE CASCADE,
        table_name          text NOT NULL,
        op_type             text NOT NULL,
        writer_class        text NOT NULL,
        mode                text,
        isolation           text,
        controlled          boolean NOT NULL DEFAULT true,
        footprint_requested text[] NOT NULL DEFAULT '{}',
        footprint_executed  text[],
        files_est           integer,
        bytes_est           bigint,
        sla_deadline        timestamptz,
        intent_at           timestamptz NOT NULL,
        decision            text,
        n_deferrals         integer NOT NULL DEFAULT 0,
        decision_delay_ms   bigint,
        scope_narrowed      boolean NOT NULL DEFAULT false,
        slot                text,
        started_at          timestamptz,
        commit_requested_at timestamptz,
        finished_at         timestamptz,
        outcome             text,       -- committed|data_conflict|retry_exhausted|commit_unknown|error|skipped
        attempts            integer,
        held_ms             bigint,
        cpu_ms              bigint,
        run_ms              bigint,
        bytes_written       bigint,
        snapshot_id         bigint,
        error_class         text,
        error_message       text,
        p_k2_pred           double precision,
        p_k1_pred           double precision,
        p_k2_analytic       double precision,
        p_k1_analytic       double precision,
        dur_pred_s          double precision,
        cost_pred_cpu_s     double precision,
        model_version       text
      );
      CREATE INDEX IF NOT EXISTS ops_run_idx ON %1$I.ops(run_id, intent_at);

      CREATE TABLE IF NOT EXISTS %1$I.decisions (
        decision_id   text PRIMARY KEY,
        run_id        text NOT NULL,
        op_id         text NOT NULL,
        at            timestamptz NOT NULL,
        kind          text NOT NULL,   -- intent|reevaluate|hold
        policy        text NOT NULL,
        action        text NOT NULL,
        delay_s       double precision,
        propensity    double precision,
        features      jsonb NOT NULL,
        candidates    jsonb NOT NULL,
        reason        text
      );
      CREATE INDEX IF NOT EXISTS decisions_run_idx ON %1$I.decisions(run_id, at);

      CREATE TABLE IF NOT EXISTS %1$I.commit_attempts (
        attempt_id       bigserial PRIMARY KEY,
        run_id           text,
        op_id            text,
        slot             text,
        writer_class     text,
        table_name       text NOT NULL,
        idempotency_key  text,
        received_at      timestamptz NOT NULL,
        forwarded_at     timestamptz,
        responded_at     timestamptz,
        hold_ms          bigint NOT NULL DEFAULT 0,
        hold_reason      text,
        http_status      integer,
        base_snapshot_id bigint,
        new_snapshot_id  bigint,
        sequence_number  bigint,
        iceberg_op       text,          -- append|overwrite|replace|delete
        op_class         text,          -- класс по conflict_matrix
        summary          jsonb,
        error_type       text           -- из тела ответа (CommitFailedException и т. п.)
      );
      CREATE INDEX IF NOT EXISTS ca_run_idx ON %1$I.commit_attempts(run_id, received_at);

      CREATE TABLE IF NOT EXISTS %1$I.commit_events (
        run_id              text,
        table_name          text NOT NULL,
        snapshot_id         bigint NOT NULL,
        parent_snapshot_id  bigint,
        sequence_number     bigint,
        committed_at        timestamptz NOT NULL,
        iceberg_op          text,
        op_class            text,
        op_id               text,
        writer_class        text,
        writer_id           text,       -- flink.job-id или slot
        added_data_files    integer,
        deleted_data_files  integer,
        added_delete_files  integer,
        added_bytes         bigint,
        n_partitions        integer,
        enriched_at         timestamptz,
        PRIMARY KEY (table_name, snapshot_id)
      );
      CREATE INDEX IF NOT EXISTS ce_run_idx ON %1$I.commit_events(run_id, committed_at);

      CREATE TABLE IF NOT EXISTS %1$I.commit_partitions (
        table_name          text NOT NULL,
        snapshot_id         bigint NOT NULL,
        partition           text NOT NULL,
        added_data_files    integer NOT NULL DEFAULT 0,
        deleted_data_files  integer NOT NULL DEFAULT 0,
        added_delete_files  integer NOT NULL DEFAULT 0,
        added_bytes         bigint NOT NULL DEFAULT 0,
        deleted_file_paths  text[],     -- только для DML/rewrite (нужны для q)
        PRIMARY KEY (table_name, snapshot_id, partition)
      );

      CREATE TABLE IF NOT EXISTS %1$I.client_reports (
        id           bigserial PRIMARY KEY,
        run_id       text,
        table_name   text,
        received_at  timestamptz NOT NULL,
        report_type  text,
        report       jsonb NOT NULL
      );

      CREATE TABLE IF NOT EXISTS %1$I.table_health (
        run_id        text NOT NULL,
        table_name    text NOT NULL,
        at            timestamptz NOT NULL,
        partition     text NOT NULL,
        data_files    integer NOT NULL,
        small_files   integer NOT NULL,
        delete_files  integer NOT NULL,
        total_bytes   bigint NOT NULL,
        PRIMARY KEY (run_id, table_name, at, partition)
      );

      CREATE TABLE IF NOT EXISTS %1$I.orphan_scan (
        run_id         text NOT NULL,
        table_name     text NOT NULL,
        at             timestamptz NOT NULL,
        orphan_files   integer NOT NULL,
        orphan_bytes   bigint NOT NULL,
        PRIMARY KEY (run_id, table_name, at)
      );

      CREATE TABLE IF NOT EXISTS %1$I.run_summary (
        run_id                 text PRIMARY KEY,
        n_ops                  integer,
        n_k2                   integer,
        n_k1_fail              integer,
        wasted_cpu_s           double precision,
        useful_cpu_s           double precision,
        wasted_bytes           bigint,
        goodput_commits_per_min double precision,
        commit_lat_p50_ms      double precision,
        commit_lat_p95_ms      double precision,
        commit_lat_p99_ms      double precision,
        delay_p95_s            double precision,
        sla_violations         integer,
        debt_mean              double precision,
        debt_max               double precision,
        orphan_bytes           bigint,
        stream_freshness_p95_s double precision,
        computed_at            timestamptz
      );

    $f$, s);
  END LOOP;
END$$;

RESET ROLE;
