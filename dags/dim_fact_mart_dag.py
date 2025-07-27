import sys

sys.path.append('/opt/airflow')
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
# from airflow.utils.dates import datetime, timedelta

from utils.alerts import task_failure_alert
from utils.bq_helpers import create_dim, append_partitioned_fact
from utils.schemas import mart_schemas
from utils.queries import mart_queries

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="dim_fact_datamart_dag",
    default_args=default_args,
    start_date=datetime(2025, 7, 22),
    schedule_interval="@daily",
    catchup=True,
    tags=["final_project", "transform"],
) as dag:

    with TaskGroup("dim_tasks") as dim_group:
        dim_tables = {
            "dim_stores": "SELECT DISTINCT store_id, store_name FROM `purwadika.jdeol003_finpro_gema.stg_stores`",
            "dim_employees": "SELECT DISTINCT employee_id, employee_name, store_id FROM `purwadika.jdeol003_finpro_gema.stg_employees`",
            "dim_products": "SELECT DISTINCT product_id, product_name, price FROM `purwadika.jdeol003_finpro_gema.stg_products`",
            "dim_customers": "SELECT DISTINCT customer_id, name AS customer_name FROM `purwadika.jdeol003_finpro_gema.stg_customers`"
        }

        for table, query in dim_tables.items():
            PythonOperator(
                task_id=table,
                python_callable=create_dim,
                op_kwargs={"table_name": table, "query": query},
                on_failure_callback=task_failure_alert
            )

    fact_task = PythonOperator(
        task_id="fact_transactions",
        python_callable=append_partitioned_fact,
        op_kwargs={
            "table_name": "fact_transactions",
            "query": """
                SELECT
                    t.transaction_id,
                    t.customer_id,
                    t.store_id,
                    t.product_id,
                    t.quantity,
                    p.price * t.quantity AS total_amount,
                    t.created_at
                FROM `purwadika.jdeol003_finpro_gema.stg_transactions` t
                JOIN `purwadika.jdeol003_finpro_gema.stg_products` p
                    ON t.product_id = p.product_id
                WHERE DATE(t.created_at) = '{{ macros.ds_add(ds, -1) }}'
            """,
            "schema": [
                {"name": "transaction_id", "type": "STRING"},
                {"name": "customer_id", "type": "STRING"},
                {"name": "store_id", "type": "STRING"},
                {"name": "product_id", "type": "STRING"},
                {"name": "quantity", "type": "INTEGER"},
                {"name": "total_amount", "type": "FLOAT"},
                {"name": "created_at", "type": "TIMESTAMP"},
            ],
            "partition_field": "created_at"
        },
        on_failure_callback=task_failure_alert
    )

    with TaskGroup("datamart_tasks") as mart_group:
        for name, query in mart_queries.items():
            PythonOperator(
                task_id=name,
                python_callable=append_partitioned_fact,
                op_kwargs={
                    "table_name": name,
                    "query": query,
                    "schema": mart_schemas[name],
                    "partition_field": "date"
                },
                on_failure_callback=task_failure_alert,
            )

    dim_group >> fact_task >> mart_group
