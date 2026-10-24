select
    "LocationID"::bigint as zone_key,
    "Borough" as borough,
    "Zone" as zone_name,
    "service_zone" as service_zone
from {{ ref('taxi_zone_lookup') }}
