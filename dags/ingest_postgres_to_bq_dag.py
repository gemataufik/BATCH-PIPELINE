from airflow import DAG
from airflow.operators.python import  ShortCircuitOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd
import os
import requests 


GCP_PROJECT_ID = "purwadika"
GCS_BUCKET_NAME = "bucket-gema"
GCS_BASE_PATH = "staging"
BQ_DATASET = "jdeol003_finpro_gema"
TABLES = ["customers", "products", "transactions", "stores", "employees"]

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': lambda context: send_telegram_alert(context),  
    'on_retry_callback': lambda context: send_telegram_alert(context),    
}


def send_telegram_alert(context):  
    import os
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    message = f"🚨 *Airflow Alert!*\n*DAG*: `{dag_id}`\n*Task*: `{task_id}`\n*Date*: `{execution_date}`\n[📄 View logs]({log_url})"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        requests.post(url, data=payload)
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")


def extract_and_upload_if_data_exists(table_name, **kwargs):
    logical_date = kwargs["logical_date"]
    target_date = (logical_date - timedelta(days=1)).date()
    
    hook = PostgresHook(postgres_conn_id="project_postgres")
    query = f"SELECT * FROM {table_name} WHERE created_at::date = '{target_date}'"
    df = hook.get_pandas_df(query)

    if df.empty:
        print(f" Skipping: No data found in {table_name} for {target_date}")
        return False  

    local_dir = f"/opt/airflow/temp/{table_name}/created_date={target_date}"
    os.makedirs(local_dir, exist_ok=True)
    local_file_path = f"{local_dir}/{table_name}.csv"
    df.to_csv(local_file_path, index=False)

    gcs_path = f"{GCS_BASE_PATH}/{table_name}/created_date={target_date}/{table_name}.csv"
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_file_path)

    print(f"Uploaded to GCS: gs://{GCS_BUCKET_NAME}/{gcs_path}")
    return True


SCHEMA_MAPPING = {
    "employees": [
        {"name": "employee_id", "type": "STRING"},
        {"name": "employee_name", "type": "STRING"},
        {"name": "store_id", "type": "STRING"},
        {"name": "created_at", "type": "TIMESTAMP"},
    ],
    "products": [
        {"name": "product_id", "type": "STRING"},
        {"name": "product_name", "type": "STRING"},
        {"name": "category", "type": "STRING"},
        {"name": "price", "type": "NUMERIC"},
        {"name": "stock", "type": "INTEGER"},
        {"name": "created_at", "type": "TIMESTAMP"},
    ],
    "stores": [
        {"name": "store_id", "type": "STRING"},
        {"name": "store_name", "type": "STRING"},
        {"name": "created_at", "type": "TIMESTAMP"},
    ],
    "customers": [
        {"name": "customer_id", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {"name": "created_at", "type": "TIMESTAMP"},
    ],
    "transactions": [
        {"name": "transaction_id", "type": "STRING"},
        {"name": "customer_id", "type": "STRING"},
        {"name": "product_id", "type": "STRING"},
        {"name": "employee_id", "type": "STRING"},
        {"name": "store_id", "type": "STRING"},
        {"name": "quantity", "type": "INTEGER"},
        {"name": "payment_method", "type": "STRING"},
        {"name": "created_at", "type": "TIMESTAMP"},
    ],
}

with DAG(
    dag_id="ingest_postgres_to_bq_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 7, 22),
    catchup=True,
    tags=["final_project", "ingestion"],
) as dag:

    for table in TABLES:
        check_and_extract = ShortCircuitOperator(
            task_id=f"check_and_extract_{table}",
            python_callable=extract_and_upload_if_data_exists,
            op_kwargs={"table_name": table},
            provide_context=True
        )

        load_to_bq = GCSToBigQueryOperator(
            task_id=f"load_{table}_to_bq",
            bucket=GCS_BUCKET_NAME,
            source_objects=[
                f"{GCS_BASE_PATH}/{table}/created_date={{{{ macros.ds_add(ds, -1) }}}}/{table}.csv"
            ],
            destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.stg_{table}",  
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            time_partitioning={
                "type": "DAY",
                "field": "created_at"
            }
        )

        check_and_extract >> load_to_bq
