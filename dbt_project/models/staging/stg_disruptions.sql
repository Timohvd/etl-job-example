-- Staging model for disruptions.
--   Input : raw.disruptions, one JSONB row per disruption per day (via the dbt source).
--   Output: the same grain (one row per disruption per snapshot day) with typed columns. History is kept on
--           purpose: the marts use it to compute first/last seen dates and daily counts.
select
    record_key,
    business_key                             as disruption_id,
    payload ->> 'type'                       as disruption_type,
    payload ->> 'title'                      as title,
    payload ->> 'priority'                   as priority,
    (payload ->> 'isActive')::boolean        as is_active,
    (payload ->> 'lastUpdated')::timestamptz as last_updated_at,
    payload ->> 'url'                        as url,
    snapshot_date,
    fetched_at,
    run_id,
    payload_hash
from {{ source('raw', 'disruptions') }}
