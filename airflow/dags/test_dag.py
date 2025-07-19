from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from pendulum import datetime
import os
import json

@dag(
    dag_id="test_dag",
    start_date=datetime(2025, 7, 1),
    schedule=None, 
    description="This is a test DAG to demonstrate the structure and functionality of Airflow DAGs.",
)
def test_dag():

    sf_client = SnowflakeHook(snowflake_conn_id="sf_test")

    @task(task_id="test_task")
    def test_task():
        # Example task that interacts with Snowflake
        query = "SELECT CURRENT_TIMESTAMP()"
        result = sf_client.get_first(query)
        print(f"Current timestamp from Snowflake: {result[0]}")
    
    @task(task_id="another_task")
    def another_task():
        # Another example task
        db_conn = json.loads(os.environ.get("AIRFLOW_CONN_SF_TEST"))
        print(f"The schema of the connection is: {db_conn["schema"]}")
    
    test_task() >> another_task()

test_dag()