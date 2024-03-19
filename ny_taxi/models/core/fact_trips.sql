{{ config(materialized="table") }}

with
    green_tripdata as (
        select
            tripid,
            vendorid,
            ratecodeid,
            passenger_count,
            pickup_locationid,
            dropoff_locationid,
            trip_distance,
            trip_type,
            fare_amount,
            store_and_fwd_flag,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            ehail_fee,
            improvement_surcharge,
            total_amount,
            payment_type,
            payment_type_description,
            'Green' as service_type
        from {{ ref("stg_green") }}
    ),
    yellow_tripdata as (
        select
            tripid,
            vendorid,
            ratecodeid,
            passenger_count,
            trip_distance,
            trip_type,
            fare_amount,
            pickup_locationid,
            dropoff_locationid,
            store_and_fwd_flag,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            ehail_fee,
            improvement_surcharge,
            total_amount,
            payment_type,
            payment_type_description,
            'Yellow' as service_type
        from {{ ref("stg_yellow") }}
    ),
    trips_unioned as (
        select *
        from green_tripdata
        union all
        select *
        from yellow_tripdata
    ),
    dim_zones as (select * from {{ ref("dim_zone") }} where borough != 'Unknown')

select
    trips_unioned.tripid,
    trips_unioned.vendorid,
    trips_unioned.service_type,
    trips_unioned.ratecodeid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    trips_unioned.store_and_fwd_flag,
    trips_unioned.passenger_count,
    trips_unioned.trip_distance,
    trips_unioned.trip_type,
    trips_unioned.fare_amount,
    trips_unioned.extra,
    trips_unioned.mta_tax,
    trips_unioned.tip_amount,
    trips_unioned.tolls_amount,
    trips_unioned.ehail_fee,
    trips_unioned.improvement_surcharge,
    trips_unioned.total_amount,
    trips_unioned.payment_type,
    trips_unioned.payment_type_description

from trips_unioned
inner join
    dim_zones as pickup_zone on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on trips_unioned.dropoff_locationid = dropoff_zone.locationid
