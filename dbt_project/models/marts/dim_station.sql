-- Mart: station dimension. One row per station, from the cleaned staging model.
-- Facts that carry a station code join to this table to get names and coordinates.
select
    station_code,
    station_name,
    station_name_short,
    eva_code,
    uic_code,
    country_code,
    station_type,
    latitude,
    longitude,
    has_departure_times,
    has_facilities,
    effective_date,
    snapshot_date as last_seen_date
from {{ ref('stg_stations') }}
