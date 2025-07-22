# Sample live-refreshing data with Airflow to simulate real world pipeline and to demo SAO

This DAG is used to simulate data ingestion from a mock API (in this instance, we are just using some Python libraries to generate mock data) and load the data into Snowflake tables. It can be used for demonstrations and development testing purposes, especially for features like Fusion's SAO capability. The Airflow instance can be started and stopped with one simple command to support any demo.

---

## 🚀 Quick Start Guide

### 1. 📦 Prerequisites

Make sure you have the following installed:

- **Python 3.x+**
- [**Astro CLI**](https://www.astronomer.io/docs/astro/cli/overview/)
    - Note that the Astro CLI will automatically install Podman as a dependency. Podman is a Docker alternative that (seems to be) more resource efficient when running on your local machine.

---

### 2. 🔧 Setup and Installation

#### Clone this repository:
https:
```
https://github.com/kevinsoenandar-dbt/live_data.git
```

or 

SSH:
```
git@github.com:kevinsoenandar-dbt/live_data.git
```

---

### 3. ⚙️ Environment Variables and Airflow Variables

#### Required Environment Variable

You must have a `.env` file in your local directory to ensure that the DAG works as expected and is able to pull in required variables. 

There are 2 environment variables currently:
1. `AIRFLOW_CONN_FKA_SNOWFLAKE_CONN`; This should be a valid Airflow connection string in JSON format that connects to Snowflake. Sample is provided below
2. `refresh_source_data_frequency_min` [OPTIONAL]; This is the frequency in **minutes** that you want to set the DAG to refresh at. If not provided, you can just trigger the DAG manually

Example:
```
AIRFLOW_CONN_FKA_SNOWFLAKE_CONN='{
  "conn_type": "snowflake",
  "login": "YOUR_USER",
  "password": "YOUR_PASSWORD",
  "extra": {
    "account": "YOUR_ACCOUNT",
    "database": "YOUR_DB",
    "warehouse": "YOUR_WAREHOUSE",
    "role": "YOUR_ROLE"
  },
  "schema": "YOUR_SCHEMA"
}'
```

For reference, I used the following settings:

```
AIRFLOW_CONN_FKA_SNOWFLAKE_CONN='{
    "conn_type": "snowflake",
    "login": "dbtlabs_ksoenandar",
    "password": "<password>",
    "schema": "dbt_ksoenandar_raw",
    "extra": {
        "account": "fka50167",
        "database": "analytics",
        "warehouse": "transforming",
        "role": "transformer"
    }
}'
```

You can also add this in the Airflow UI under Admin → Connections.

### 4. 🧪 What This DAG Does

This DAG:
1. Checks connection to Snowflake.
2. Checks if necessary tables exist.
3. Creates missing tables (if any).
4. Seeds mock data using `MockData` class.
5. Uploads and loads `.csv.gz` files into Snowflake using SQL scripts.
6. Cleans up local and staged files after loading.

---

### 5. 🗃️ File Structure Overview

```text
dags/
└── refresh_source_data.py     # Main DAG logic

include/
├── sql/                       # SQL scripts used in the DAG
│   ├── create_tables.sql
│   ├── check_conn.sql
│   ├── sample_table.sql
│   ├── stage_files.sql
│   └── copy_data.sql
├── scripts/
│   ├── api/
│   │   ├── mock_data.py       # Generates mock data
│   │   └── mock_schema.py     # Defines table schemas
└── generated_data/            # Temporary folder for generated CSVs
```

---

### 6. 🧹 Clean-up

After the DAG finishes:
- Files in `include/generated_data/` (except `products.csv`) are deleted locally. The `products.csv` file is stored to be re-used for subsequent runs.
- Snowflake stage `@~/sao/` is cleaned up. This stage lives in your Snowflake's user stage. 

---

### 7. 🛠️ Tips for Development

- Start the Airflow instance with the `astro dev start` command.
- You can use `astro dev restart` to restart Airflow after code changes.
- Use the Airflow UI (`http://localhost:8080`) to monitor the DAG run.
- Use logs to troubleshoot any operator failures.

---

### ✅ That's it!

You're now ready to use and customize the `refresh_source_data` DAG for your own development or demo purposes.