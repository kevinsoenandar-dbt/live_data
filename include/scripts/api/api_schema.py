schema = {
    "customers": """
        id string,
        first_name string,
        last_name string,
        email string,
        gender string,
        state string,
        city string,
        loaded_at timestamp_ntz
    """,
    "products": """
        id string,
        name string,
        category string,
        subcategory string,
        price float,
        product_cost float
    """,
    "orders": """
        id string,
        customer_id string,
        order_date date,
        order_status string,
        loaded_at timestamp_ntz
    """,
    "order_products": """
        id string,
        product_id string,
        order_id string,
        quantity int,
        loaded_at timestamp_ntz
    """
}