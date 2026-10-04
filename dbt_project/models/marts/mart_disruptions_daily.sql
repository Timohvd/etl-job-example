-- Mart: active disruptions per snapshot day and type. Days with no active disruptions produce no row.
select
    snapshot_date,
    disruption_type,
    count(*)                                    as active_disruptions,
    count(*) filter (where priority = 'PRIO_1') as prio_1_disruptions
from {{ ref('stg_disruptions') }}
where is_active
group by snapshot_date, disruption_type
