with orders as (
    select * from {{ ref("stg_bike_shop__orders") }}
)

select * exclude (customer_id, loaded_at)

from orders