with bounds as (
    select
        min(pickup_date)::date as min_date,
        max(pickup_date)::date as max_date
    from {{ ref('stg_yellow_taxi_trips') }}
),

spine as (
    select unnest(generate_series(min_date, max_date, interval 1 day))::date as date_key
    from bounds
    where min_date is not null and max_date is not null
)

select
    date_key,
    year(date_key) as year,
    month(date_key) as month,
    day(date_key) as day_of_month,
    dayname(date_key) as day_name,
    isodow(date_key) as iso_day_of_week,
    date_trunc('week', date_key)::date as week_start_date,
    date_trunc('month', date_key)::date as month_start_date
from spine
