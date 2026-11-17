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

-- Shared metrics whose active bindings disagree on how readings should be interpreted.
CREATE OR REPLACE VIEW metric_binding_conflicts AS
SELECT
  metric_id,
  count(*) AS active_bindings,
  collect_set(data_type) AS data_types,
  collect_set(quality_system) AS quality_systems
FROM metric_catalog
WHERE is_active
GROUP BY metric_id
HAVING count(DISTINCT data_type) > 1 OR count(DISTINCT quality_system) > 1;
