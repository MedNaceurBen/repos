with source as (
    select * 
    from {{ source('dev','weather_report') }}
),

renamed as (
    select 
        id,
        city,
        temperature,
        weather_descriptions,
        wind_speed,
        time as weather_time_local,
        (inserted_at + (utc_offset || 'hours')::interval) inserted_at_local
    from source
)

select *
from renamed