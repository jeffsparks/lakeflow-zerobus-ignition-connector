-- Diagnostic views over the historian tables. Replace <catalog> and <schema>.

USE CATALOG <catalog>;
USE SCHEMA <schema>;

-- metric_ids observed with more than one identity. The pipeline does not register a metric_id whose
-- identity is ambiguous or differs from its registry entry; such metrics appear here instead.
CREATE OR REPLACE VIEW metric_identity_conflicts AS
SELECT
  metric_id,
  collect_set(named_struct('identity_mode', identity_mode, 'identity_key', identity_key)) AS identities
FROM (
  SELECT metric_id, identity_mode, identity_key FROM metric_metadata_events
  UNION
  SELECT metric_id, identity_mode, identity_key FROM metric_registry
)
WHERE metric_id IS NOT NULL
GROUP BY metric_id
HAVING count(*) > 1;

-- Development aid: metrics published under more than one active name, with what those names report. Two
-- publishers of one metric is a configuration problem; data type or quality system disagreements are the serious
-- cases. Each name's state is its highest-revision snapshot, as in metric_names.
CREATE OR REPLACE VIEW metric_name_conflicts AS
WITH latest AS (
  SELECT * FROM (
    SELECT *, row_number() OVER (
      PARTITION BY producer_id, tag_provider, tag_path
      ORDER BY metadata_revision DESC, metadata_observed_at DESC) AS rn
    FROM metric_metadata_events
    WHERE metric_id IS NOT NULL AND producer_id IS NOT NULL AND tag_path IS NOT NULL AND metadata_revision IS NOT NULL
  )
  WHERE rn = 1 AND is_active
)
SELECT
  metric_id,
  count(*) AS active_names,
  count(DISTINCT producer_id) AS active_producers,
  collect_set(named_struct('producer_id', producer_id, 'tag_provider', tag_provider, 'tag_path', tag_path)) AS names,
  collect_set(data_type) AS data_types,
  collect_set(quality_system) AS quality_systems,
  collect_set(named_struct('history_enabled', history_enabled, 'history_max_age_ms', history_max_age_ms)) AS history_settings
FROM latest
GROUP BY metric_id
HAVING count(*) > 1;
