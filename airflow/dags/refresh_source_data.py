from airflow.sdk import dag, task, task_group, chain, Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import os
import json
from pendulum import now, datetime, duration

import logging
logger = logging.getLogger(__name__)

from include.scripts.api.mock_data import MockData
from include.scripts.api.mock_schema import schema

def check_tables_existence_results(ti):
    # This function is written to support the branching logic; we only want to create tables if they don't currently exist in the user's database and schema combination
    if len(ti.xcom_pull(task_ids="check_tables_existence", key="return_value")) == 0:
        return "create_tables"
    else: 
        return "get_existing_customers"

db_conn = json.loads(os.environ.get("AIRFLOW_CONN_FKA_SNOWFLAKE_CONN"))
    
@dag(
    dag_id="refresh_source_data",
    start_date=datetime(2025, 7, 1),
    schedule=duration(minutes=int(Variable.get(key="refresh_source_data_frequency_min"))), # Adjust this as needed; bear in mind that the entire DAG takes about 2 minutes to run usually
    description="This is a pipeline that refreshes the source data from a mock API and writes it to a Snowflake table. Useful to demo the SAO capability of Fusion.",
    template_searchpath=[os.path.join(os.getcwd(), "include", "sql")],
    catchup=False,
    max_active_runs=1
)
def refresh_source_data():

    client = MockData()
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

    @task(task_id="get_initial_data")
    def get_initial_data(ti):
        client.seed_initial_data()
        ti.xcom_push(key="initial_run", value=True)
    
    @task(task_id="get_regular_data")
    def get_regular_data(ti):
        xcom_message = ti.xcom_pull(key="return_value", task_ids="get_existing_customers")
        existing_customers = [item[0] for item in xcom_message]
        client.refresh_data(existing_customers, 1000, 950)
        ti.xcom_push(key="initial_run", value=False)

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

    # Step 4: Get existing customers to populate new orders with SOME existing customers
    get_existing_customers = SQLExecuteQueryOperator(
        task_id = "get_existing_customers",
        sql="sample_table.sql",
        params={
            "database": db_conn["extra"]["database"],
            "schema": db_conn["schema"],
            "table_name": "customers"
        },
        conn_id="fka_snowflake_conn"
    )

    # These empty operators are created to ensure that the chaining logic can work correctly
    empty_task = EmptyOperator(task_id="empty_task", trigger_rule="none_failed")

    @task(task_id="get_files_list")
    def get_files_list(ti):
        initial_run = ti.xcom_pull(key="initial_run", task_ids="get_initial_data") or ti.xcom_pull(key="initial_run", task_ids="get_regular_data")
        logger.info(f"Is an initial run: {initial_run}")
        for dir, subdir, files in os.walk(os.path.join(os.getcwd(), "include", "generated_data")):
            if not initial_run:
                # Products dataset is static and so there's no need to refresh it constantly. 
                files.remove("products.csv")
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
        bash_command="cd /usr/local/airflow && find ./include/generated_data/ ! -name 'products.csv' -type f -exec rm -f {} +"
    )

    chain(check_conn(), check_tables, branch_check, [create_tables(), get_existing_customers], [get_initial_data(), get_regular_data()], 
          empty_task)
    files_list = get_files_list()
    stage_files = stage_file.expand(file_name=files_list)
    empty_task >> files_list >> stage_files >> copy_file.expand(file_name=stage_files) >> remove_staged_files >> clean_local_files

refresh_source_data()