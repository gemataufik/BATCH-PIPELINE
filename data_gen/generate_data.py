from faker import Faker
import random
import uuid

fake = Faker()

CATEGORIES = ["Minuman", "Makanan", "Elektronik", "Perawatan Tubuh", "Kebutuhan Rumah"]
PAYMENT_METHODS = ["Cash", "QRIS", "Transfer Bank", "E-Wallet"]

STORE_CODES = ["JK-01", "BD-02", "SB-03", "ML-04", "YK-05", "BA-06"]
STORE_NAMES = ["Jkt_Mart", "Bdg_Mart", "Sby_Mart", "Mlg_Mart", "Ygt_Mart", "Bi_Mart"]

def generate_customer(created_at):
    return (
        str(uuid.uuid4()),
        fake.name(),
        fake.unique.email(),
        created_at
    )

def generate_product(index, created_at):
    code = f"{fake.lexify(text='??').upper()}-{index+1}"
    return (
        code,
        fake.word().capitalize(),
        random.choice(CATEGORIES),
        round(random.uniform(5.0, 200.0), 2),
        random.randint(20, 100),
        created_at
    )

def generate_store(code, name, created_at):
    return (
        code,
        name,
        created_at
    )

def generate_employee(store_id, created_at):
    return (
        str(uuid.uuid4()),
        fake.name(),
        store_id,
        created_at
    )

def generate_transaction(customer_ids, product_list, employee_ids, created_at):
    customer_id = random.choice(customer_ids)
    product = random.choice(product_list)
    product_id = product[0]
    quantity = random.randint(1, 5)
 
    payment_method = random.choice(PAYMENT_METHODS)
    employee_id = random.choice(employee_ids)
    store_id = random.choice(STORE_CODES)

    return (
        str(uuid.uuid4()),
        customer_id,
        product_id,
        employee_id,
        store_id,
        quantity,
        payment_method,
        created_at,
    )
