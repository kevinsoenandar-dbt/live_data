import os
import polars
import logging
from faker import Faker
from scipy.stats import norm

import random
from pendulum import now, date, duration

logger = logging.getLogger(__name__)

class MockData:
    def __init__(self):
        self.fake = Faker()

    def get_product_data(self, initial_run: bool = True) -> polars.DataFrame:
        """
        Function to obtain product data
        
        Parameters:
        initial_run (bool): whether this is the first DAG run

        In the initial run, this reads off the seed data. It then generates the ID field and inserted a randomised product cost + loaded at timestamp.
        In subsequent runs, this reads off the previously cleaned data.
        """
        if initial_run:
            df = polars.read_csv(os.path.join(os.getcwd(), "include", "seed_data", "products.csv"))
            ids = polars.Series("id", [self.fake.uuid4() for _ in range(len(df))])
            df = df.insert_column(0, ids)
            df = df.with_columns(
                (polars.col("price") * random.uniform(0.3, 0.5)).alias("product_cost"),
                polars.lit(now(tz="UTC").to_rfc3339_string()).alias("loaded_at")
            )
            df = df.rename({"category1": "category", "category2": "subcategory"})
        else:
            df = polars.read_csv(os.path.join(os.getcwd(), "include", "generated_data", "products.csv"), separator="\t")

        return df
    
    def build_customer_data(self, num_customers: int = 1000) -> polars.DataFrame:
        customers = []
        for _ in range(num_customers):
            customer = {
                "id": self.fake.uuid4(),
                "first_name": self.fake.first_name(),
                "last_name": self.fake.last_name(),
                "email": self.fake.email(),
                "gender": self.fake.passport_gender(),
                "city": self.fake.city(),
                "loaded_at": now(tz="UTC").to_rfc3339_string()
            }
            customers.append(customer)
        df = polars.DataFrame(customers)
        return df
    
    def build_order_data(self, customer_ids: list, num_orders: int = 1000) -> polars.DataFrame:
        orders = []
        for order_num in range(num_orders):
            order = {
                "id": self.fake.uuid4(),
                "customer_id": customer_ids[order_num],
                "order_date": random.choice([(now(tz="UTC").date() - duration(days=i)).strftime("%Y-%m-%d") for i in range(30)]), # Use last 30 days as at each DAG run
                "order_status": random.choices(["shipped", "delivered", "refunded"], [10, 5, 1])[0],
                "loaded_at": now(tz="UTC").to_rfc3339_string()
            }
            orders.append(order)
        df = polars.DataFrame(orders)
        return df
    
    def calculate_product_weight(self, products_df: polars.DataFrame) -> polars.DataFrame:
        """
        Calculate product purchase probability based on price. The closer the product price is to the median price of all products, the more likely it will appear in the
        order_products dataset
        """
        median_price = products_df["price"].median()
        std_price = products_df["price"].std()
        output = products_df.with_columns(
            polars.col("price").map_elements(
                lambda price: norm.pdf(price, loc=median_price, scale=std_price,),
                return_dtype=float
            ).alias("probability_weight")
        )
        return output
    
    def build_order_products_data(self, products_df: polars.DataFrame, orders_df: polars.DataFrame) -> polars.DataFrame:
        order_products = []
        products = self.calculate_product_weight(products_df)
        product_ids = products["id"].to_list()
        product_id_weights = products["probability_weight"].to_list()
        orders = orders_df

        # Assume some orders will be multi-item orders as opposed to just a single item.
        orders = orders.with_columns(
            polars.Series("unique_products", random.choices([1, 2, 3], weights=[5, 2, 1], k=len(orders))
    )
        )
        for order in orders.iter_rows(named=True):
            print(order["unique_products"])
            for i in range(order["unique_products"]):
                order_product = {
                    "id": self.fake.uuid4(),
                    "product_id": random.choices(product_ids, product_id_weights)[0],
                    "order_id": order["id"],
                    "quantity": random.choices([1, 2, 3], [10, 3, 1])[0], # Randomised quantity for each product, between 1 - 3 items, with most orders coming in at the lowest quantity
                    "loaded_at": now(tz="UTC").to_rfc3339_string()
                }
                order_products.append(order_product)
        df = polars.DataFrame(order_products)
        return df

    def write_csvs(self, dfs: dict[str, polars.DataFrame]) -> None:
        for file, df in dfs.items():
            full_path = os.path.join(os.getcwd(), "include", "generated_data", file + ".csv")
            df.write_csv(full_path, separator="\t")
        
    def seed_initial_data(self) -> None:
        """
        This function will (in order):
        1. Read product data from the seed file
        2. Create a random initial customers dataset
        3. Create a random initial orders dataset
        4. Create a random initial order_products dataset
        """
        products = self.get_product_data()
        customers = self.build_customer_data()
        orders = self.build_order_data(customers["id"])
        order_products = self.build_order_products_data(products, orders)
        
        self.write_csvs({
            "products": products,
            "customers": customers,
            "orders": orders,
            "order_products": order_products
        })

    def refresh_data(self, existing_customers: list, num_orders: int = 1000, num_customers: int = 950) -> None:
        """
        This function will (in order):
        1. Read product data from the generated_data folder
        2. Create a new set of customer dataset, with SOME existing customers thrown in for "returning" customers concept
        3. Create a new set of orders dataset
        4. Create a new set of order_products dataset
        """
        products = self.get_product_data(initial_run=False)
        customers = self.build_customer_data()

        customer_list = customers["id"].to_list() + existing_customers
        orders = self.build_order_data(customer_list)
        order_products = self.build_order_products_data(products, orders)
        
        self.write_csvs({
            "products": products,
            "customers": customers,
            "orders": orders,
            "order_products": order_products
        })
        