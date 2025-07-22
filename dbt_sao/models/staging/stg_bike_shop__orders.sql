with source as (
    select * from {{ source("bike_shop", "orders") }}
),

renamed as (

    select

        ----------  ids
        id as order_id,
        customer_id,

        ---------- text
        initcap(order_status) as order_status,
        
        ---------- date
        order_date,

        ---------- timestamp
        loaded_at

    from source
)

select * from renamed
