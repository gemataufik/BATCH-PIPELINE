from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

from data_gen.generate_data import (
    generate_customer,
    generate_product,
    generate_transaction,
    generate_store,
    generate_employee,
    STORE_CODES,
    STORE_NAMES
)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def create_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            store_id TEXT PRIMARY KEY,
            store_name VARCHAR(100),
            created_at TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            employee_id UUID PRIMARY KEY,
            employee_name VARCHAR(100),
            store_id TEXT REFERENCES stores(store_id),
            created_at TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id UUID PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            created_at TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            product_name VARCHAR(100),
            category VARCHAR(100),
            price NUMERIC,
            stock INTEGER,
            created_at TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id UUID PRIMARY KEY,
            customer_id UUID REFERENCES customers(customer_id),
            product_id TEXT REFERENCES products(product_id),
            employee_id UUID REFERENCES employees(employee_id),
            store_id TEXT REFERENCES stores(store_id),
            quantity INTEGER,
            payment_method VARCHAR(50),
            created_at TIMESTAMP
        );
    """)

def insert_all_data(**kwargs):
    created_at = kwargs['logical_date']

    hook = PostgresHook(postgres_conn_id='project_postgres')
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            create_tables(cursor)


            cursor.execute("SELECT COUNT(*) FROM stores;")
            if cursor.fetchone()[0] == 0:
                store_ids = []
                for code, name in zip(STORE_CODES, STORE_NAMES):
                    store = generate_store(code, name, created_at)
                    store_ids.append(store[0])
                    cursor.execute("""
                        INSERT INTO stores (store_id, store_name, created_at)
                        VALUES (%s, %s, %s)
                    """, store)
            else:
                cursor.execute("SELECT store_id FROM stores")
                store_ids = [row[0] for row in cursor.fetchall()]


            cursor.execute("SELECT COUNT(*) FROM employees;")
            if cursor.fetchone()[0] == 0:
                employee_ids = []
                for store_id in store_ids:
                    for _ in range(5):
                        employee = generate_employee(store_id, created_at)
                        employee_ids.append(employee[0])
                        cursor.execute("""
                            INSERT INTO employees (employee_id, employee_name, store_id, created_at)
                            VALUES (%s, %s, %s, %s)
                        """, employee)
            else:
                cursor.execute("SELECT employee_id FROM employees")
                employee_ids = [row[0] for row in cursor.fetchall()]


            cursor.execute("SELECT COUNT(*) FROM products;")
            product_list = []
            if cursor.fetchone()[0] < 30:
                for i in range(30):
                    product = generate_product(i, created_at)
                    product_list.append(product)
                    cursor.execute("""
                        INSERT INTO products (product_id, product_name, category, price, stock, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (product_id) DO NOTHING
                    """, product)
            else:
                cursor.execute("SELECT * FROM products;")
                product_list = cursor.fetchall()


            for _ in range(15):
                cursor.execute("""
                    INSERT INTO customers (customer_id, name, email, created_at)
                    VALUES (%s, %s, %s, %s)
                """, generate_customer(created_at))


            cursor.execute("SELECT customer_id FROM customers;")
            customer_ids = [row[0] for row in cursor.fetchall()]
            for _ in range(20):
                transaction = generate_transaction(customer_ids, product_list, employee_ids, created_at)
                cursor.execute("""
                    INSERT INTO transactions (
                        transaction_id, customer_id, product_id,
                        employee_id, store_id, quantity,
                        payment_method, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, transaction)

        conn.commit()

with DAG(
    dag_id='insert_data_toko_dag',
    default_args=default_args,
    description='Insert simulated retail data into PostgreSQL',
    schedule_interval='@hourly',
    start_date=datetime(2025, 7, 21),
    catchup=False
) as dag:

    insert_task = PythonOperator(
        task_id='generate_and_insert_data',
        python_callable=insert_all_data,
    )
