-- Plausibility check: every Dutch station must lie inside a bounding box around the Netherlands.
-- Catches swapped latitude/longitude, unit changes and nulls that a plain not_null test would not flag as wrong.
select station_code, latitude, longitude
from {{ ref('stg_stations') }}
where latitude  not between 50.5 and 53.8
   or longitude not between 3.0  and 7.4
   or latitude is null
   or longitude is null
