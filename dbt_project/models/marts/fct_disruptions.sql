-- Mart: disruption facts. Collapses the daily snapshots into one row per disruption.
--   * Descriptive fields (type, priority, title) come from the MOST RECENT snapshot, so a value NS changed
--     over time is never an arbitrary pick.
--   * first_seen_date / last_seen_date / days_observed describe when this pipeline observed the disruption;
--     they are not NS's own start/end times.
with snapshots as (

    select * from {{ ref('stg_disruptions') }}

),

latest_per_disruption as (

    select
        *,
        row_number() over (partition by disruption_id order by snapshot_date desc) as rn
    from snapshots

),

observed as (

    select
        disruption_id,
        min(snapshot_date)            as first_seen_date,
        max(snapshot_date)            as last_seen_date,
        count(distinct snapshot_date) as days_observed
    from snapshots
    group by disruption_id

),

latest_day as (

    select max(snapshot_date) as snapshot_date from snapshots

)

select
    l.disruption_id,
    l.disruption_type,
    l.priority,
    l.title,
    l.last_updated_at,
    o.first_seen_date,
    o.last_seen_date,
    o.days_observed,
    (l.is_active and l.snapshot_date = d.snapshot_date) as is_currently_active
from latest_per_disruption as l
inner join observed as o using (disruption_id)
cross join latest_day as d
where l.rn = 1
