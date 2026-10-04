-- Staging model for stations.
--   Input : raw.stations, one JSONB row per station per day (via the dbt source).
--   Output: one row per station = the latest snapshot, JSON unpacked and cast to real types.
-- `->>` reads a JSON field as text, `->` descends into a nested object, `::type` casts the value.
with ranked as (
    select
        *,
        row_number() over (
            -- Number each station's rows newest to oldest; rn = 1 is the latest snapshot.
            partition by business_key
            order by snapshot_date desc, fetched_at desc
        ) as rn
    from {{ source('raw', 'stations') }}
)

select
    record_key,
    business_key                                as station_code,
    payload ->> 'EVACode'                       as eva_code,
    payload ->> 'UICCode'                       as uic_code,
    payload -> 'namen' ->> 'lang'               as station_name,
    payload -> 'namen' ->> 'kort'               as station_name_short,
    payload ->> 'land'                          as country_code,
    payload ->> 'stationType'                   as station_type,
    (payload ->> 'lat')::double precision       as latitude,
    (payload ->> 'lng')::double precision       as longitude,
    (payload ->> 'heeftVertrektijden')::boolean as has_departure_times,
    (payload ->> 'heeftFaciliteiten')::boolean  as has_facilities,
    (payload ->> 'ingangsDatum')::date          as effective_date,
    snapshot_date,
    fetched_at,
    run_id,
    payload_hash
from ranked
where rn = 1
