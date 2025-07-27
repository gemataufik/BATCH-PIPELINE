from google.cloud import bigquery
from datetime import timedelta
from google.cloud.exceptions import Conflict

PROJECT_ID = "purwadika"
DATASET = "jdeol003_finpro_gema"
bq_client = bigquery.Client()

def create_dim(table_name, query, **kwargs):
    bq_client.query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.{table_name}` AS
        {query}
    """).result()

def create_partitioned_table_if_needed(table_name, schema, partition_field):
    try:
        table_ref = bigquery.Table(f"{PROJECT_ID}.{DATASET}.{table_name}", schema=schema)
        table_ref.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
        bq_client.create_table(table_ref)
    except Conflict:
        pass

def append_partitioned_fact(table_name, query, execution_date, schema=None, partition_field=None, **kwargs):
    date_str = (execution_date - timedelta(days=1)).date()
    query_filled = query.replace("{{ ds }}", str(date_str))

    if schema and partition_field:
        create_partitioned_table_if_needed(table_name, schema, partition_field)

    job_config = bigquery.QueryJobConfig(
        destination=f"{PROJECT_ID}.{DATASET}.{table_name}",
        write_disposition="WRITE_APPEND"
    )
    bq_client.query(query_filled, job_config=job_config).result()
