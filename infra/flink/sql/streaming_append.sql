-- Потоковая загрузка Kafka → Iceberg (append). Отправляется runner через Flink SQL Gateway.
-- Плейсхолдеры {{...}} подставляются runner из сценария (runner/streaming.py).

SET 'execution.checkpointing.interval' = '{{commit_period_s}}s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'pipeline.name' = 'capl-stream-{{stream_id}}-{{run_id}}';
SET 'parallelism.default' = '{{parallelism}}';

CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = '{{catalog_uri}}',
  'warehouse' = '{{warehouse}}',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = '{{s3_endpoint}}',
  's3.path-style-access' = 'true',
  'header.X-CAPL-Writer' = 'stream',
  'header.X-CAPL-Run' = '{{run_id}}'
);

CREATE TEMPORARY TABLE src_events (
  event_id BIGINT,
  `key`    BIGINT,
  dt       DATE,
  event_ts TIMESTAMP(6),
  amount   DOUBLE,
  status   STRING,
  version  INT,
  payload  STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '{{topic}}',
  'properties.bootstrap.servers' = '{{kafka_bootstrap}}',
  'properties.group.id' = 'capl-{{stream_id}}-{{run_id}}',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- Писатель идентифицируется прокси по `flink.job-id` в summary снапшота (добавляется Iceberg автоматически)
-- и по заголовку X-CAPL-Writer каталога.
INSERT INTO lake.bench.events
SELECT event_id, `key`, dt, event_ts, amount, status, version, payload FROM src_events;
