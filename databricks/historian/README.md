# Historian tables and pipeline

The connector (`sinkMode=zerobus`) writes three Zerobus landing tables; a Lakeflow pipeline maintains the
processed tables.

## Tags, PLC tags, and metrics

- A **PLC tag** is a value inside an industrial device, usually a register in a PLC. It can represent anything,
  even the previous value of another PLC tag. Ignition reads the device over a native protocol (EtherNet/IP,
  Modbus TCP, ...) through a device connection and exposes each PLC tag on its internal OPC UA server, addressed
  by an OPC item path such as `ns=1;s=[filler1_plc1]Status`.
- A **tag** is an Ignition tag (usually on Ignition Edge). An engineer names it freely and points it at a PLC tag
  through its OPC item path. Ignition timestamps each value when it reads the PLC tag. A tag can be renamed or
  pointed at a different PLC tag at any time, and several tags can point at the same PLC tag, intentionally or
  not.
- A **metric** is the stored timeseries of a tag's values, identified by `metric_id`. Each tag path that
  publishes a metric is one of its names, kept in `metric_names`.

## Name-keyed and source-keyed metrics

Contextualizing industrial data means making assumptions about what a PLC tag represents. The historian keys
each metric on whichever part is reliable.

**Name-keyed metrics** are well-characterized, standard data points with a governed name, such as the PackML
machine state. The metric is the name. If an engineer later finds the tag reads the wrong PLC tag, they point it
at the right one and the metric continues; its source does not matter. Renaming the tag creates a different
metric, because a standard name is a different data point.

**Source-keyed metrics** are raw signals whose meaning is not settled, such as a temperature that might be the
motor's or the shaft's. Engineers place these tags in a `raw` folder, and the source-keyed tag regex (for
example `/raw/`) selects them. The metric is the PLC tag within its work unit, and tag names are labels on it:

- Renaming the tag (motor temperature to shaft temperature) keeps the metric's history.
- Several tags pointing at the same PLC tag are one metric with several names, and the data is stored once.
  This is normal and expected; Ignition doesn't warn about it and it can't be prevented.
- Pointing the tag at a different PLC tag moves the name to that PLC tag's metric.

Moving a tag out of `raw` into a standard name makes it name-keyed, which starts a new metric.

| Table | Written by | Purpose |
|---|---|---|
| `metrics_ingest` | connector (Zerobus) | Append-only reading landing table; may contain retries |
| `metric_metadata_events` | connector (Zerobus) | Append-only metadata snapshots, one per change of a producer/provider/path; the audit history of every name and setting |
| `collector_events` | connector (Zerobus) | Collector startup, heartbeat, and shutdown events |
| `metrics` | pipeline | Permanent deduplicated readings |
| `metric_registry` | pipeline | Permanent `metric_id` → `metric_num` mapping |
| `metric_names` | pipeline | Current state of every name a metric is published under (producer + provider + tag path) |
| `metric_metadata` | pipeline | One resolved row per `metric_id`: history settings, data type, units, description, `is_active` |

## How data lands and is processed

1. The connector reads its tag selection every **Metadata Check Interval**. It appends a snapshot to
   `metric_metadata_events` when a tag is first seen, when any of its settings change, and when the tag is marked
   as no longer existing.
2. For each history-enabled tag, the connector appends readings to `metrics_ingest` (see
   [Readings sent by the connector](#readings-sent-by-the-connector)). With **Log All Selected Tags (Ignore
   History Enabled)** checked, every selected tag is collected; the snapshots still record each tag's own
   History Enabled value in `history_enabled`.
3. The pipeline copies new readings into `metrics`, dropping duplicates.
4. For each new metadata snapshot, the pipeline:
   - registers a new `metric_id` in `metric_registry`;
   - updates that name's row in `metric_names`;
   - re-resolves the metric's row in `metric_metadata`.

The connector only appends to the landing tables. Every other table is derived by the pipeline.

## Contracts

**metric_id** is the lowercase hex SHA-1 of the UTF-8 `identity_key`:

- The **metric name** is the tag path without the `[provider]` prefix. In folder selection mode with
  **Include Root Folder in Metric Names** unchecked, the name starts below the Tag Folder Path. For example,
  with root `[MQTT Engine]spb_telemetry`, the tag `spb_telemetry/site/area/Tag` is named `site/area/Tag`. The
  hierarchy levels and `device_tag_name` are parsed from the name. `tag_path` always holds the full path.
- `name` mode: `identity_key` is the metric name, case-sensitive.
- `source` mode: `identity_key` is `<first four non-leaf segments of the name>::<source_ref>` (fewer segments
  for shallower names). Applies when the source-keyed rule is enabled, its regex is found in the full tag path
  (including `[provider]`), and the tag has a source reference. The connector reads it from `opcItemPath`, or
  from the tag property named in **OPC Item Path Property** (for example a custom property `cp-opcItemPath` on
  MQTT Engine tags, which do not carry the edge tag's `opcItemPath`).
  This works only for edge tags outside UDT instances. MQTT Transmission publishes a UDT instance as a
  Sparkplug template, and MQTT Engine rebuilds it from the UDT definition and the instance's parameters, so
  properties set on an instance's tags never reach the Standard gateway. Those tags have no source reference
  and are name-keyed.

Other producers can write the same landing tables if they follow these rules.

**Readings** are duplicates only when `metric_id`, `time`, `quality`, and the typed value are all equal
(NULL-safe across every `value_*` column). The pipeline keeps the earliest `ingest_time`.
`time` is the reading's edge timestamp, and `ingest_time` is the landing commit time in Databricks.

**Metadata** snapshots carry a `metadata_revision` that increases for each producer/provider/path. A
`metric_names` row changes only when a snapshot carries the highest revision seen for that name.

**is_active** on a name is true while the name is in its producer's latest complete declaration. For the
connector, that is its latest complete scan of the tag selection. A producer going offline does not make its names
inactive. Narrowing the tag selection marks the excluded tags inactive.

**metric_metadata** is resolved from the latest snapshot of each of the metric's active names. A source-keyed
metric often has several names, each with its own settings; a name-keyed metric has exactly one. A metric
should have only one publisher (`producer_id`); two can happen only by mistake. These rules combine the names'
settings, and give a predictable result even then:

- `history_enabled`: true if any active name has history enabled.
- `history_max_age_ms`: the shortest positive interval among history-enabled names. If there is none, the value
  of the most recently changed name. 0 or NULL means no max-age readings.
- `data_type`, `quality_system`, `engineering_units`, `description`: from the most recently changed active name.
- `is_active`: true if any name is active. When no name is active, the last resolved settings are kept.

## Readings sent by the connector

- **On change.** A history-enabled tag sends a reading on every value or quality change, with the tag's own
  timestamp and quality.
- **Max-age readings.** If a history-enabled tag's last value was Good and nothing has been sent for longer than
  its Ignition **Max Time Between Samples** (minimum 1 second), the connector re-sends that value. The reading is
  timestamped when it is sent and has quality **203 (Good_Backfill)**, so it can be told apart from a real
  change. A value that is not Good stops max-age readings until the next Good value. Max-age readings are made by
  the connector from its own tag settings, not by the pipeline.
- **Tags no longer present.** A tag missing from one complete scan is still collected. A tag missing from two
  complete scans in a row is marked inactive (an `is_active = false` snapshot), and its collection stops. A scan
  that fails or can't read the whole selection does not count.
- **Startup Delay.** The first tag scan after the collector starts waits this long (default 0 ms). Use it when tag
  providers from other modules, such as MQTT Engine, need time to load after a gateway restart.

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
   `metric_registry`, `metric_names`, and `metric_metadata`.
4. Configure the connector's target schema (`catalog.schema`) and enable it.

## Operational notes

- **Pipeline full refresh.** A full refresh resets only the pipeline checkpoints. The processed tables are
  never truncated by the pipeline, and replayed rows are absorbed by the deduplicating writes. Do not drop
  or truncate `metric_registry`: its `metric_num` values are permanent.
- **ingest_time without change data feed.** If change data feed is unavailable on `metrics_ingest`, set
  `historian.ingest_time_source=file`. The pipeline then uses the landing file's modification time.
- **Diagnostic views.** `metric_identity_conflicts` lists `metric_id`s observed with more than one identity.
  These are not registered automatically. `metric_name_conflicts` (development aid) lists metrics with more
  than one active name, along with their producers, data types, quality systems, and history settings.
- **Zerobus target tables.** Zerobus rejects tables with column defaults, identity columns, or generated
  columns, so the three landing tables have none.
