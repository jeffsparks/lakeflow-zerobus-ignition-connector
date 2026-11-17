# Historian tables and pipeline

The connector (`sinkMode=zerobus`) writes three Zerobus landing tables; a Lakeflow pipeline maintains the
processed tables.

| Table | Written by | Purpose |
|---|---|---|
| `metrics_ingest` | connector (Zerobus) | Append-only reading landing table; may contain retries |
| `metric_metadata_events` | connector (Zerobus) | Append-only metadata snapshots per producer/provider/path binding |
| `collector_events` | connector (Zerobus) | Collector startup, heartbeat, and shutdown events |
| `metrics` | pipeline | Permanent deduplicated readings |
| `metric_registry` | pipeline | Permanent `metric_id` → `metric_num` mapping |
| `metric_catalog` | pipeline | Current state of each binding |

## Contracts

**metric_id** is the lowercase hex SHA-1 of the UTF-8 `identity_key`:

- `name` mode: `identity_key` is the tag path without the `[provider]` prefix, case-sensitive.
- `source` mode: `identity_key` is `<first four non-leaf path segments>::<source_ref>` (fewer segments for
  shallower paths). Applies when the source-keyed rule is enabled, its regex is found in the full tag path
  (including `[provider]`), and the tag has a source reference such as `opcItemPath`.

Other producers can write the same landing tables if they follow these rules.

**Readings** are duplicates only when `metric_id`, `time`, `quality`, and the typed value are all equal
(NULL-safe across every `value_*` column). The pipeline keeps the earliest `ingest_time`.
`time` is the reading's edge timestamp, and `ingest_time` is the landing commit time in Databricks.

**Metadata** snapshots carry a `metadata_revision` that increases for each binding. The catalog row for a
binding changes only when a snapshot carries the highest revision seen for that binding.

## Deployment order

1. Run `tables.sql`, then `views.sql`.
2. Grant the connector's service principal `USE CATALOG`, `USE SCHEMA`, and `SELECT, MODIFY` on
   `metrics_ingest`, `metric_metadata_events`, and `collector_events`.
3. Create a Lakeflow pipeline in **continuous** mode with `historian_pipeline.py` as its source and these
   configuration values:
   - `historian.catalog` and `historian.schema`: the target catalog and schema.
   - `historian.ingest_time_source`: optional. Defaults to `commit`.

   ForEachBatch sinks are in Public Preview and may require the pipeline's **Preview** channel. The
   pipeline's run-as identity needs `SELECT` on the landing tables and `SELECT, MODIFY` on `metrics`,
   `metric_registry`, and `metric_catalog`.
4. Configure the connector's target schema (`catalog.schema`) and enable it.

## Operational notes

- **Pipeline full refresh.** A full refresh resets only the pipeline checkpoints. The processed tables are
  never truncated by the pipeline, and replayed rows are absorbed by the deduplicating writes. Do not drop
  or truncate `metric_registry`: its `metric_num` values are permanent.
- **ingest_time without change data feed.** If change data feed is unavailable on `metrics_ingest`, set
  `historian.ingest_time_source=file`. The pipeline then uses the landing file's modification time.
- **Diagnostic views.** `metric_identity_conflicts` lists `metric_id`s observed with more than one identity.
  These are not registered automatically. `metric_binding_conflicts` lists shared metrics whose active
  bindings disagree on `data_type` or `quality_system`.
- **Zerobus target tables.** Zerobus rejects tables with column defaults, identity columns, or generated
  columns, so the three landing tables have none.
