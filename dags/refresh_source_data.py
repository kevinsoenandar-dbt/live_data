from airflow.sdk import dag, task, task_group, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import os

from pendulum import now, datetime, duration
from include.scripts.api.mock_api import MockApi
from include.scripts.api.api_schema import schema

def check_tables_existence_results(**kwargs):
    if kwargs["ti"].xcom_pull(task_ids="check_tables_existence", key="return_value") == []:
        return "create_tables"
    else: 
        return "empty_task_1"
    
@dag(
    dag_id="refresh_source_data",
    start_date=datetime(2025, 7, 1),
    schedule=None,
    description="This is a pipeline that refreshes the source data from a mock API and writes it to a Snowflake table. Useful to demo the SAO capability of Fusion.",
    template_searchpath=[os.path.join(os.getcwd(), "include", "sql")]
)
def refresh_source_data():

    client = MockApi()
    sf_client = SnowflakeHook(snowflake_conn_id="fka_snowflake_conn")

    @task(task_id="check_conn")
    def check_conn():
        with open("include/sql/check_conn.sql", "r") as file:
            sql = file.read()
        sf_client.run(sql=sql)

    check_tables = SQLExecuteQueryOperator(
        task_id="check_tables_existence",
        sql="check_tables.sql",
        params={"database": os.environ.get("SF_DATABASE"), "schema": os.environ.get("SF_SCHEMA")},
        conn_id="fka_snowflake_conn",
        split_statements=True,
        show_return_value_in_logs=True,
        do_xcom_push=True
    )

    @task_group(group_id="create_tables")
    def create_tables():
        create_customers_table = SQLExecuteQueryOperator(
            task_id="create_customers_table",
            sql="create_tables.sql",
            params={
                "database": os.environ.get("SF_DATABASE"),
                "schema": os.environ.get("SF_SCHEMA"),
                "table_name": "customers",
                "definitions": schema["customers"]
            },
            conn_id="fka_snowflake_conn"
        )

        create_products_table = SQLExecuteQueryOperator(
            task_id="create_products_table",
            sql="create_tables.sql",
            params={
                "database": os.environ.get("SF_DATABASE"),
                "schema": os.environ.get("SF_SCHEMA"),
                "table_name": "products",
                "definitions": schema["products"]
            },
            conn_id="fka_snowflake_conn"
        )

        create_orders_table = SQLExecuteQueryOperator(
            task_id="create_orders_table",
            sql="create_tables.sql",
            params={
                "database": os.environ.get("SF_DATABASE"),
                "schema": os.environ.get("SF_SCHEMA"),
                "table_name": "orders",
                "definitions": schema["orders"]
            },
            conn_id="fka_snowflake_conn"
        )

        create_order_products_table = SQLExecuteQueryOperator(
            task_id="create_order_products_table",
            sql="create_tables.sql",
            params={
                "database": os.environ.get("SF_DATABASE"),
                "schema": os.environ.get("SF_SCHEMA"),
                "table_name": "order_products",
                "definitions": schema["order_products"]
            },
            conn_id="fka_snowflake_conn"
        )

        [create_orders_table, create_products_table, create_customers_table, create_order_products_table]


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

        get_customers_data() >> get_orders_data() >> get_order_products_data()

    @task_group(group_id="get_first_batch_data")
    def get_first_batch_data():
        @task(task_id="get_products_data")
        def get_products_data():
            products_df = client.get("products")
            client.write_to_csv(products_df, file_name="products.csv")
        
        get_products_data() >> get_regular_data()

    check_conn = SQLExecuteQueryOperator(
            task_id = "check_conn",
            sql = "check_conn.sql",
            conn_id="fka_snowflake_conn",
            show_return_value_in_logs=True
        )
    
    branch_check = BranchPythonOperator(
        task_id="branch_check",
        python_callable=check_tables_existence_results
    )

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
            database=os.environ.get("SF_DATABASE"),
            schema=os.environ.get("SF_SCHEMA"),
            table=file_name,
            file_name=file_name + ".csv.gz")
        )
        
    # stage_files = [
    #     SQLExecuteQueryOperator(
    #         task_id=f"stage_{file_name}",
    #         sql="stage_files.sql",
    #         params={
    #             "database": os.environ.get("SF_DATABASE"),
    #             "schema": os.environ.get("SF_SCHEMA"),
    #             "file_name": file_name
    #         },
    #         conn_id="fka_snowflake_conn"
    #     ) for file_name in ti.xcom_pull(task_ids="get_files_list", key="file_list")
    # ]

    # copy_files = [
    #     SQLExecuteQueryOperator(
    #         task_id=f"copy_{file_name}",
    #         sql="copy_data.sql",
    #         params={
    #             "database": os.environ.get("SF_DATABASE"),
    #             "schema": os.environ.get("SF_SCHEMA"),
    #             "table": file_name,
    #             "file_name": file_name
    #         },
    #         conn_id="fka_snowflake_conn"
    #     ) for file_name in ti.xcom_pull(task_ids="get_files_list", key="file_list")
    # ]

    remove_staged_files = SQLExecuteQueryOperator(
        task_id="remove_staged_files",
        sql="remove @~/sao/",
        conn_id="fka_snowflake_conn"
    )

    clean_local_files = BashOperator(
        task_id="clean_files", 
        bash_command="cd /usr/local/airflow && rm -rf include/downloaded_data/*.csv"
    )

    chain(check_conn, check_tables, branch_check, [create_tables(), empty_task_1], [get_first_batch_data(), get_regular_data()], 
          empty_task_2)
    files_list = get_files_list()
    stage_files = stage_file.expand(file_name=files_list)
    empty_task_2 >> files_list >> stage_files >> copy_file.expand(file_name=stage_files) >> remove_staged_files >> clean_local_files

refresh_source_data()