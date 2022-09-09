from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import psycopg2
import sys
import requests

sys.path.append('/home/shavel/PycharmProjects/Airflow')
from db_config import con


def get_data_usd():
    print('Hello World!')
    with con.cursor() as cur:
        cur.execute(
            """SELECT * 
                INTO TABLE sold_1 
                FROM orders 
                WHERE currency = 'usd' 
                ORDER BY id ASC""")
        if con:
            con.close()
            print('Connection closed')


def get_data_non_usd():
    with con.cursor() as cur:
        cur.execute(
            """SELECT *
                FROM orders
                WHERE currency = 'byn'
                ORDER BY id ASC""")
        desc = cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row)) for row in cur.fetchall()]
        convert(data)


def convert(data):
    for i in data:
        amount = i.get('price')
        api = 'https://v6.exchangerate-api.com/v6/7dbce18c99cd7ea949727872/pair/BYN/USD/'
        url = api + str(amount)
        responce = requests.get(url)
        c = responce.json()
        i['price'] = c['conversion_result']
        i['currency'] = 'usd'
        val = tuple(i.values())
        write_to_db(val)


def write_to_db(val):
    with con.cursor() as cur:
        cur.execute("""
        INSERT INTO sold_1 (id, product_name, price, currency, purchase_date)
        VALUES (%s, %s, %s, %s, %s);
        """, (val))


with DAG(
        dag_id='test_dag',
        # default_args=default_args,
        description='my db dag',
        start_date=datetime(2022, 9, 5),
        catchup=False,
        schedule_interval='@daily',
        tags=['test_dag']
) as dag:
    task1 = PythonOperator(task_id='Get_data_with_USD', python_callable=get_data_usd)

    task2 = PythonOperator(task_id='Get_data_non_USD', python_callable=get_data_non_usd)

    task1 >> task2