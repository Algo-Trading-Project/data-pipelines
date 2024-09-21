from airflow import DAG
from operators.get_order_book_data_operator import GetOrderBookDataOperator

from datetime import timedelta
import pendulum

schedule_interval = timedelta(days = 1, hours = 1)
start_date = pendulum.datetime(
    year = 2023,
    month = 8,
    day = 15,
    tz = 'America/Los_Angeles'
)

with DAG(
    dag_id = 'fetch_order_book_data_1h',
    start_date = start_date,
    schedule_interval = schedule_interval,
    max_active_runs =  1,
    catchup = False
) as dag:
    
    order_book_data_1h_to_s3 = GetOrderBookDataOperator(
        task_id = 'order_book_data_1h_to_s3'
    )

    order_book_data_1h_to_s3
