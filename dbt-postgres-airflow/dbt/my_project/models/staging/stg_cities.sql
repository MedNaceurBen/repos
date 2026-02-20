{{ config(materialized='view') }}

with source as (
    select * 
    from {{ source('dev','city_info') }}
),

renamed as (
    select 
        id::integer as city_id,
        name::text as city_name,
        country::text,
        latitude::numeric,
        longitude::numeric
    from source
)

select *
from renamed