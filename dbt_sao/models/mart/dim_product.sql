with products as (
    select * from {{ ref("stg_bike_shop__products") }}
)

select * exclude (product_cost,product_price,loaded_at)

from products