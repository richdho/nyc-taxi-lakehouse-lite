with trips as (
    select *
    from {{ ref('stg_yellow_taxi_trips') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by
                pickup_datetime,
                dropoff_datetime,
                vendor_id,
                pu_location_id,
                do_location_id,
                passenger_count,
                trip_distance,
                total_amount
            order by source_file
        ) as trip_dup_rank
    from trips
)

select
    hash(
        pickup_datetime,
        dropoff_datetime,
        vendor_id,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        total_amount,
        trip_dup_rank
    ) as trip_sk,
    pickup_date as date_key,
    pu_location_id as pickup_zone_key,
    do_location_id as dropoff_zone_key,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    trip_duration_seconds,
    ratecode_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    trip_month,
    source_file
from deduped
