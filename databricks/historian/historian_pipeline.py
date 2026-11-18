"""Lakeflow pipeline source for the Zerobus historian.

Flows:
  metrics_ingest (change data feed) -> metrics
  metric_metadata_events            -> metric_registry, metric_names, metric_metadata

The targets are ordinary managed tables written through ForEachBatch sinks, so a pipeline full refresh
resets only the checkpoints; replayed rows are absorbed by the same deduplicating writes.

Pipeline configuration keys:
  historian.catalog                       (required)
  historian.schema                        (required)
  historian.ingest_time_source            "commit" (default): change data feed _commit_timestamp
                                          "file": landing file modification time (when CDF is unavailable)
  historian.metrics_ingest.starting_version  first metrics_ingest version to read with "commit" (default 0)
  historian.readings_trigger_interval     default "5 seconds"
  historian.metadata_trigger_interval     default "10 seconds"
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("historian.catalog")
SCHEMA = spark.conf.get("historian.schema")
INGEST_TIME_SOURCE = spark.conf.get("historian.ingest_time_source", "commit")
STARTING_VERSION = spark.conf.get("historian.metrics_ingest.starting_version", "0")
READINGS_TRIGGER = spark.conf.get("historian.readings_trigger_interval", "5 seconds")
METADATA_TRIGGER = spark.conf.get("historian.metadata_trigger_interval", "10 seconds")


def table(name):
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


VALUE_COLS = ["value_int", "value_double", "value_bool", "value_text", "value_ts", "value_bytes", "value_json"]
READING_KEY = ["metric_id", "time", "quality"] + VALUE_COLS
METRICS_COLS = ["metric_id", "time", "ingest_time", "quality"] + VALUE_COLS

NAME_KEY = ["producer_id", "tag_provider", "tag_path"]
NAME_COLS = [
    "metric_id", "producer_id", "tag_provider", "tag_path", "is_active", "hierarchy", "device_tag_name",
    "source_ref", "metadata_observed_at",
]
METADATA_COLS = [
    "metric_id", "is_active", "history_enabled", "history_max_age_ms", "data_type", "quality_system",
    "engineering_units", "description", "metadata_observed_at",
]


# ---------------------------------------------------------------------------
# Readings
# ---------------------------------------------------------------------------

@dp.foreach_batch_sink(name="metrics_sink")
def write_metrics(df, batch_id):
    # Duplicates share metric_id + time + quality + typed value (NULL-safe); ingest_time is not part of
    # the key and the earliest one is kept. groupBy treats NULLs as equal, matching <=> below.
    readings = (
        df.where("metric_id IS NOT NULL AND time IS NOT NULL AND quality IS NOT NULL")
        .groupBy(*READING_KEY)
        .agg(F.min("ingest_time").alias("ingest_time"))
    )
    bounds = readings.agg(
        F.min(F.unix_micros("time")).alias("lo"), F.max(F.unix_micros("time")).alias("hi")
    ).first()
    if bounds is None or bounds["lo"] is None:
        return

    readings.select(*METRICS_COLS).createOrReplaceTempView("historian_readings_batch")
    match = " AND ".join(
        ["t.metric_id = s.metric_id", "t.time = s.time", "t.quality = s.quality"]
        + [f"t.{c} <=> s.{c}" for c in VALUE_COLS]
        # Exact bounds of this batch; only narrows the target scan, never excludes a candidate match.
        + [f"t.time BETWEEN timestamp_micros({bounds['lo']}) AND timestamp_micros({bounds['hi']})"]
    )
    df.sparkSession.sql(
        f"""
        MERGE INTO {table('metrics')} t
        USING historian_readings_batch s
        ON {match}
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def _landing_readings():
    if INGEST_TIME_SOURCE == "file":
        return (
            spark.readStream.table(table("metrics_ingest"))
            .withColumn("ingest_time", F.col("_metadata.file_modification_time"))
        )
    return (
        spark.readStream.option("readChangeFeed", "true")
        .option("startingVersion", STARTING_VERSION)
        .table(table("metrics_ingest"))
        .where("_change_type = 'insert'")
        .withColumn("ingest_time", F.col("_commit_timestamp"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )


@dp.append_flow(target="metrics_sink", spark_conf={"pipelines.trigger.interval": READINGS_TRIGGER})
def metrics_ingest_readings():
    return _landing_readings()


# ---------------------------------------------------------------------------
# Metadata: identity registration, current names, resolved metric metadata
# ---------------------------------------------------------------------------

def _register_identities(events):
    session = events.sparkSession
    identities = (
        events.where("identity_mode IS NOT NULL AND identity_key IS NOT NULL")
        .select("metric_id", "identity_mode", "identity_key")
        .distinct()
    )
    # A metric_id seen with two identities in one batch is ambiguous and is not registered; it, and any
    # metric_id whose identity differs from its existing registry row, surfaces in metric_identity_conflicts.
    unambiguous = (
        identities.groupBy("metric_id")
        .agg(
            F.count("*").alias("n"),
            F.first("identity_mode").alias("identity_mode"),
            F.first("identity_key").alias("identity_key"),
        )
        .where("n = 1")
        .drop("n")
    )
    new_ids = unambiguous.join(session.table(table("metric_registry")).select("metric_id"), "metric_id", "left_anti")
    new_ids.createOrReplaceTempView("historian_new_identities")
    # Plain insert: the identity column assigns metric_num; this flow is the registry's only writer.
    session.sql(
        f"""
        INSERT INTO {table('metric_registry')} (metric_id, identity_mode, identity_key)
        SELECT metric_id, identity_mode, identity_key FROM historian_new_identities
        """
    )


def _update_names(events):
    session = events.sparkSession
    newest_in_batch = (
        events.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy(*NAME_KEY).orderBy(
                    F.desc("metadata_revision"), F.desc("metadata_observed_at")
                )
            ),
        )
        .where("rn = 1")
        .drop("rn")
    )
    # Apply a snapshot only if it is the highest revision ever recorded for its name, so late or
    # replayed older snapshots never overwrite newer state.
    history = session.table(table("metric_metadata_events")).where("metadata_revision IS NOT NULL")
    max_revision = (
        history.alias("h")
        .join(
            newest_in_batch.select(*NAME_KEY).alias("b"),
            (F.col("h.producer_id") == F.col("b.producer_id"))
            & F.col("h.tag_provider").eqNullSafe(F.col("b.tag_provider"))
            & (F.col("h.tag_path") == F.col("b.tag_path")),
            "left_semi",
        )
        .groupBy(*NAME_KEY)
        .agg(F.max("metadata_revision").alias("max_revision"))
    )
    current = (
        newest_in_batch.alias("n")
        .join(
            max_revision.alias("m"),
            (F.col("n.producer_id") == F.col("m.producer_id"))
            & F.col("n.tag_provider").eqNullSafe(F.col("m.tag_provider"))
            & (F.col("n.tag_path") == F.col("m.tag_path")),
            "left",
        )
        .where("m.max_revision IS NULL OR n.metadata_revision >= m.max_revision")
        .select(*[F.col(f"n.{c}") for c in NAME_COLS])
    )
    current.createOrReplaceTempView("historian_names_batch")
    assignments = ", ".join(f"t.{c} = s.{c}" for c in NAME_COLS)
    session.sql(
        f"""
        MERGE INTO {table('metric_names')} t
        USING historian_names_batch s
        ON t.producer_id = s.producer_id AND t.tag_provider <=> s.tag_provider AND t.tag_path = s.tag_path
        WHEN MATCHED THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN INSERT ({", ".join(NAME_COLS)}) VALUES ({", ".join("s." + c for c in NAME_COLS)})
        """
    )


def _resolve_metadata(events):
    """Re-resolves metric_metadata for every metric that a name in this batch points at now or pointed at before.

    A name's current state is its highest-revision snapshot in metric_metadata_events (the rule metric_names
    applies), so the result does not depend on batch order and a replay reproduces it.
    """
    session = events.sparkSession
    events.select(*NAME_KEY).distinct().createOrReplaceTempView("historian_changed_names")
    resolved = session.sql(
        f"""
        WITH snapshots AS (
          SELECT * FROM {table('metric_metadata_events')}
          WHERE metric_id IS NOT NULL AND producer_id IS NOT NULL AND tag_path IS NOT NULL
            AND metadata_revision IS NOT NULL
        ),
        affected AS (
          SELECT DISTINCT e.metric_id FROM snapshots e
          LEFT SEMI JOIN historian_changed_names c
            ON e.producer_id = c.producer_id AND e.tag_provider <=> c.tag_provider AND e.tag_path = c.tag_path
        ),
        names AS (
          SELECT DISTINCT e.producer_id, e.tag_provider, e.tag_path FROM snapshots e
          LEFT SEMI JOIN affected a ON e.metric_id = a.metric_id
        ),
        latest AS (
          SELECT * FROM (
            SELECT e.*, row_number() OVER (
              PARTITION BY e.producer_id, e.tag_provider, e.tag_path
              ORDER BY e.metadata_revision DESC, e.metadata_observed_at DESC) AS rn
            FROM snapshots e
            LEFT SEMI JOIN names n
              ON e.producer_id = n.producer_id AND e.tag_provider <=> n.tag_provider AND e.tag_path = n.tag_path
          ) WHERE rn = 1
        ),
        current AS (
          SELECT l.*,
            bool_or(l.is_active) OVER (PARTITION BY l.metric_id) AS any_active,
            struct(l.metadata_observed_at, l.metadata_revision, l.producer_id, coalesce(l.tag_provider, ''),
                   l.tag_path) AS recency
          FROM latest l
          LEFT SEMI JOIN affected a ON l.metric_id = a.metric_id
        ),
        -- Active names decide; a metric without any falls back to its inactive names.
        resolved AS (
          SELECT
            metric_id,
            coalesce(bool_or(history_enabled), false) AS history_enabled,
            coalesce(
              min(CASE WHEN history_enabled AND history_max_age_ms > 0 THEN history_max_age_ms END),
              max_by(history_max_age_ms, recency)) AS history_max_age_ms,
            max_by(data_type, recency) AS data_type,
            max_by(quality_system, recency) AS quality_system,
            max_by(engineering_units, recency) AS engineering_units,
            max_by(description, recency) AS description,
            max(metadata_observed_at) AS metadata_observed_at
          FROM current
          WHERE is_active OR NOT any_active
          GROUP BY metric_id
        ),
        activity AS (
          SELECT metric_id, bool_or(is_active) AS is_active FROM current GROUP BY metric_id
        )
        SELECT
          a.metric_id,
          coalesce(x.is_active, false) AS is_active,
          r.history_enabled, r.history_max_age_ms, r.data_type, r.quality_system, r.engineering_units,
          r.description, r.metadata_observed_at,
          r.metric_id IS NOT NULL AS has_names
        FROM affected a
        LEFT JOIN resolved r ON r.metric_id = a.metric_id
        LEFT JOIN activity x ON x.metric_id = a.metric_id
        """
    )
    resolved.createOrReplaceTempView("historian_metadata_batch")
    assignments = ", ".join(f"t.{c} = s.{c}" for c in METADATA_COLS)
    # A metric with no active name keeps its last resolved settings; one seen for the first time is
    # inserted from its inactive names.
    session.sql(
        f"""
        MERGE INTO {table('metric_metadata')} t
        USING historian_metadata_batch s
        ON t.metric_id = s.metric_id
        WHEN MATCHED AND s.is_active THEN UPDATE SET {assignments}
        WHEN MATCHED AND t.is_active THEN UPDATE SET t.is_active = false
        WHEN NOT MATCHED AND s.has_names
          THEN INSERT ({", ".join(METADATA_COLS)}) VALUES ({", ".join("s." + c for c in METADATA_COLS)})
        """
    )


@dp.foreach_batch_sink(name="metadata_sink")
def write_metadata(df, batch_id):
    events = df.where(
        "metric_id IS NOT NULL AND producer_id IS NOT NULL AND tag_path IS NOT NULL "
        "AND metadata_revision IS NOT NULL"
    )
    if events.isEmpty():
        return
    events = events.cache()
    try:
        _register_identities(events)
        _update_names(events)
        _resolve_metadata(events)
    finally:
        events.unpersist()


@dp.append_flow(target="metadata_sink", spark_conf={"pipelines.trigger.interval": METADATA_TRIGGER})
def metric_metadata_snapshots():
    return spark.readStream.table(table("metric_metadata_events"))
