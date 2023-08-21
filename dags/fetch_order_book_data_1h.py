from airflow import DAG
from operators.get_order_book_data_operator import GetOrderBookDataOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

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

    order_book_data_1h_cols = [
        'symbol_id', 'time_exchange', 'time_coinapi', 'asks', 'bids',
        'asset_id_base', 'asset_id_quote', 'exchange_id'
    ]

    s3_order_book_data_1h_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_order_book_data_1h_to_redshifts',
        schema = 'coinapi',
        table = 'order_book_data_1h',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/order_book_data',
        redshift_conn_id = 'token_price_database_conn',
        aws_conn_id = 's3_conn',
        method = 'UPSERT',
        upsert_keys = ['exchange_id', 'asset_id_base', 'asset_id_quote', 'time_exchange', 'time_coinapi'],
        copy_options = ["json 'auto'", "TIMEFORMAT 'auto'"],
        column_list = order_book_data_1h_cols
    )

    order_book_data_1h_to_s3 >> s3_order_book_data_1h_to_redshift
