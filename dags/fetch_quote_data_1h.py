from airflow import DAG
from operators.get_quote_data_operator import GetQuoteDataOperator
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
    dag_id = 'fetch_quote_data_1h',
    start_date = start_date,
    schedule_interval = schedule_interval,
    max_active_runs =  1,
    catchup = False
) as dag:
    
    quote_data_1h_to_s3 = GetQuoteDataOperator(
        task_id = 'quote_data_1h_to_s3'
    )

    quote_data_1h_cols = [
        'symbol_id', 'time_exchange', 'time_coinapi', 'ask_price',
        'ask_size', 'bid_price', 'bid_size', 'asset_id_base', 
        'asset_id_quote', 'exchange_id'
    ]

    s3_quote_data_1h_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_quote_data_1h_to_redshift',
        schema = 'coinapi',
        table = 'quote_data_1h',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/quote_data',
        redshift_conn_id = 'token_price_database_conn',
        aws_conn_id = 's3_conn',
        method = 'UPSERT',
        upsert_keys = ['exchange_id', 'asset_id_base', 'asset_id_quote', 'time_exchange', 'time_coinapi'],
        copy_options = ["json 'auto'", "TIMEFORMAT 'auto'"],
        column_list = quote_data_1h_cols
    )

    quote_data_1h_to_s3 >> s3_quote_data_1h_to_redshift
