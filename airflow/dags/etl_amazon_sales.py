from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
import os

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Paths in container environment
csv_file_path = "/opt/airflow/local_project/amazon_sales_data/amazon.csv"
temp_file = "/opt/airflow/local_project/temp/amazon_sales_data.json"
gcp_credentials_path = "/opt/airflow/gcp_credentials.json"
dbt_project_path = "/opt/airflow/dbt_project"  # Match your volume mount path

# GCP configuration
project_id = "firstetlpipeline-444201"
dataset_id = "amazonsales"
table_id = "amazon_sales_data"  #set this to what ever you want the table name to be within the amazonsales dataset in bigquery

# Functions
def extract_csv_to_temp(**kwargs):
    """Extract data from CSV and prepare for BigQuery upload."""
    df = pd.read_csv(csv_file_path)
    os.makedirs(os.path.dirname(temp_file), exist_ok=True)  # Ensure the temp directory exists
    df.to_json(temp_file, orient="records", lines=True)
    return temp_file

def load_data_to_bigquery(temp_file, **kwargs):
    """Load the JSON file to BigQuery."""
    client = bigquery.Client.from_service_account_json(gcp_credentials_path)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Load data
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,    #autodetects table schema
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(temp_file, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete

    print(f"Loaded {load_job.output_rows} rows into {table_ref}.")

# DAG Definition
with DAG(
    dag_id="etl_amazon_sales",
    default_args=default_args,
    description="ETL pipeline for Amazon sales data",
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    # Extract Task
    extract_task = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_csv_to_temp,
    )

    # Load Task
    load_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_data_to_bigquery,
        op_kwargs={"temp_file": temp_file},
    )

    # Transform Task
    transform_task = BashOperator(
        task_id="run_dbt_transformations",
        bash_command=(
            'export PATH="/home/airflow/.local/bin:$PATH" && '
            'export DBT_PROFILES_DIR="/home/airflow/.dbt" && '
            f'cd "{dbt_project_path}" && '
            'mkdir -p /home/airflow/.dbt && '
            'dbt --version && '
            'dbt deps --profiles-dir /home/airflow/.dbt && '
            'dbt seed --profiles-dir /home/airflow/.dbt && '
            'dbt run --profiles-dir /home/airflow/.dbt && '
            'dbt test --profiles-dir /home/airflow/.dbt'
        ),
        env={
            "DBT_PROFILES_DIR": "/home/airflow/.dbt",
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/gcp_credentials.json",
            "HOME": "/home/airflow"
        }
    )

# Task dependencies
extract_task >> load_task >> transform_task
