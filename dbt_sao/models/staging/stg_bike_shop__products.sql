with source as (
    select * from {{ source("bike_shop", "products") }}
),

renamed as (

    select

        ----------  ids
        id as product_id,

        ---------- text
        model as product_name,
        frame as product_material,
        category as product_category,
        subcategory as product_subcategory,

        ---------- numbers
        price as product_price,
        round(product_cost, 2) as product_cost,

        ---------- timestamp
        loaded_at

    from source
)

select * from renamed
