-- Consistency check: a disruption cannot be observed on more days than lie between its first and last sighting.
select disruption_id, first_seen_date, last_seen_date, days_observed
from {{ ref('fct_disruptions') }}
where days_observed > (last_seen_date - first_seen_date) + 1
