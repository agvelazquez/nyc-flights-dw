{{ config(materialized='view') }}

with airports as 
(
  select *,
    row_number() over(partition by faa) as rn
  from {{ source('staging','nyc_airports') }}
  where faa is not null 
)
select
    cast(faa as varchar(3)) as faa,
    cast(name as varchar) as airport_name,
    cast(latitude as numeric) as latitude,
    cast(longitude as numeric) as  longitude,
    cast(altitude as integer) as altitude,
    cast(timezone as integer) as timezone,
    cast(dst as varchar(1)) as dst,
    cast(timezone_name as varchar) as timezone_name
from airports
where rn = 1


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}