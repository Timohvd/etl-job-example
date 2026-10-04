-- Volume check: fails if the number of stations is implausible.
-- A singular test returns the rows that VIOLATE the rule; no rows means pass.
-- Why: if the API returns 3 stations instead of ~400, every generic test (unique, not_null) still passes, but
-- the data is clearly wrong. A volume check catches silent partial loads.
select count(*) as station_count
from {{ ref('stg_stations') }}
having count(*) not between 300 and 1500
