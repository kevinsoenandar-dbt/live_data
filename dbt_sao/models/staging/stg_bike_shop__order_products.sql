with source as (
    select * from {{ source("bike_shop", "order_products") }}
),

renamed as (

    select

        ----------  ids
        id as order_product_id,
        product_id,
        order_id,

        ---------- numbers
        quantity as order_product_quantity,

        ---------- timestamp
        loaded_at

    from source
)

select * from renamed
