from airflow.sdk import dag, task, task_group, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import os
import json
from pendulum import now, datetime, duration

from include.scripts.api.mock_api import MockApi
from include.scripts.api.api_schema import schema

def check_tables_existence_results(**kwargs):
    # This function is written to support the branching logic; we only want to create tables if they don't currently exist in the user's database and schema combination
    if kwargs["ti"].xcom_pull(task_ids="check_tables_existence", key="return_value") == []:
        return "create_tables"
    else: 
        return "empty_task_1"

db_conn = json.loads(os.environ.get("AIRFLOW_CONN_FKA_SNOWFLAKE_CONN"))
    
@dag(
    dag_id="refresh_source_data",
    start_date=datetime(2025, 7, 1),
    schedule=None, # Adjust this as needed; bear in mind that the entire DAG takes about 2 minutes to run usually
    description="This is a pipeline that refreshes the source data from a mock API and writes it to a Snowflake table. Useful to demo the SAO capability of Fusion.",
    template_searchpath=[os.path.join(os.getcwd(), "include", "sql")]
)
def refresh_source_data():

    client = MockApi()
    sf_client = SnowflakeHook(snowflake_conn_id="fka_snowflake_conn")

    @task_group(group_id="create_tables")
    def create_tables():
        create_customers_table = SQLExecuteQueryOperator(
            task_id="create_customers_table",
            sql="create_tables.sql",
            params={
                "database": db_conn["extra"]["database"],
                "schema": db_conn["schema"],
                "table_name": "customers",
                "definitions": schema["customers"]
            },
            conn_id="fka_snowflake_conn"
        )

        create_products_table = SQLExecuteQueryOperator(
            task_id="create_products_table",
            sql="create_tables.sql",
            params={
                "database": db_conn["extra"]["database"],
                "schema": db_conn["schema"],
                "table_name": "products",
                "definitions": schema["products"]
            },
            conn_id="fka_snowflake_conn"
        )

        create_orders_table = SQLExecuteQueryOperator(
            task_id="create_orders_table",
            sql="create_tables.sql",
            params={
                "database": db_conn["extra"]["database"],
                "schema": db_conn["schema"],
                "table_name": "orders",
                "definitions": schema["orders"]
            },
            conn_id="fka_snowflake_conn"
        )

        create_order_products_table = SQLExecuteQueryOperator(
            task_id="create_order_products_table",
            sql="create_tables.sql",
            params={
                "database": db_conn["extra"]["database"],
                "schema": db_conn["schema"],
                "table_name": "order_products",
                "definitions": schema["order_products"]
            },
            conn_id="fka_snowflake_conn"
        )

        [create_orders_table, create_products_table, create_customers_table, create_order_products_table]

    # these tasks are grouped together to ensure that it can be reused in the two different branches of the DAG (first run and subsequent runs)
    @task_group(group_id="get_regular_data")
    def get_regular_data():
        @task(task_id="get_customers_data")
        def get_customers_data():
            customers_df = client.get("customers")
            client.write_to_csv(customers_df, file_name="customers.csv")
        
        @task(task_id="get_orders_data")
        def get_orders_data():
            current_time = now()
            params = {
                "min_order_date": (current_time - duration(days=30)).strftime("%m/%d/%Y"),
                "max_order_date": (current_time - duration(hours=1)).strftime("%m/%d/%Y")
            }
            orders_df = client.get("orders",params=params)
            client.write_to_csv(orders_df, file_name="orders.csv")
    
        @task(task_id="get_order_products_data")
        def get_order_products_data():
            order_products_df = client.get("order-product")
            client.write_to_csv(order_products_df, file_name="order_products.csv")

        # Customers need to always be fetched first to ensure that the orders data can obtain the correct customer IDs and orders need 
        # to be fetched before order_products to ensure that the order IDs are correct
        get_customers_data() >> get_orders_data() >> get_order_products_data()

    @task_group(group_id="get_first_batch_data")
    def get_first_batch_data():
        @task(task_id="get_products_data")
        def get_products_data():
            products_df = client.get("products")
            client.write_to_csv(products_df, file_name="products.csv")
        
        # Likewise, products need to be fetched first to ensure that subsequent order_products can be obtained with the correct product IDs
        # We only fetch this for the first run, no point in updating the products as frequently as the other tables
        get_products_data() >> get_regular_data()

    # Step 1: Check if the connection to Snowflake is working
    @task(task_id="check_conn")
    def check_conn():
        with open("include/sql/check_conn.sql", "r") as file:
            sql = file.read()
        sf_client.run(sql=sql)

    # Step 2: Check if the tables already exist in the user's database and schema combination
    check_tables = SQLExecuteQueryOperator(
        task_id="check_tables_existence",
        sql="check_tables.sql",
        params={"database": db_conn["extra"]["database"], "schema": db_conn["schema"]},
        conn_id="fka_snowflake_conn",
        split_statements=True,
        show_return_value_in_logs=True,
        do_xcom_push=True
    )
    
    # Step 3: Branching logic to determine whether to create tables or not
    branch_check = BranchPythonOperator(
        task_id="branch_check",
        python_callable=check_tables_existence_results
    )

    # These empty operators are created to ensure that the chaining logic can work correctly
    empty_task_1 = EmptyOperator(task_id="empty_task_1")
    empty_task_2 = EmptyOperator(task_id="empty_task_2", trigger_rule="none_failed")

    @task(task_id="get_files_list")
    def get_files_list(ti):
        for dir, subdir, files in os.walk(os.path.join(os.getcwd(), "include", "downloaded_data")):
            return [file[:len(file)-4] for file in files]

    @task(task_id="stage_file")
    def stage_file(file_name: str):
        with open("include/sql/stage_files.sql", "r") as file:
            sql = file.read()

        sf_client.run(sql=sql.format(file_name=file_name))

        return file_name
    
    @task(task_id="copy_file")
    def copy_file(file_name: str):
        with open("include/sql/copy_data.sql", "r") as file:
            sql = file.read()

        sf_client.run(sql=sql.format(
            database= db_conn["extra"]["database"],
            schema= db_conn["schema"],
            table=file_name,
            file_name=file_name + ".csv.gz")
        )

    remove_staged_files = SQLExecuteQueryOperator(
        task_id="remove_staged_files",
        sql="remove @~/sao/",
        conn_id="fka_snowflake_conn"
    )

    clean_local_files = BashOperator(
        task_id="clean_files", 
        bash_command="cd /usr/local/airflow && rm -rf include/downloaded_data/*.csv"
    )

    chain(check_conn(), check_tables, branch_check, [create_tables(), empty_task_1], [get_first_batch_data(), get_regular_data()], 
          empty_task_2)
    files_list = get_files_list()
    stage_files = stage_file.expand(file_name=files_list)
    empty_task_2 >> files_list >> stage_files >> copy_file.expand(file_name=stage_files) >> remove_staged_files >> clean_local_files

refresh_source_data()