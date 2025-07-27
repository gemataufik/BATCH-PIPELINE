FROM apache/airflow:2.8.1

USER airflow

RUN pip install --upgrade pip && \
    pip install \
    psycopg2-binary \
    faker \
    google-cloud-storage \
    google-cloud-bigquery \
    pandas
