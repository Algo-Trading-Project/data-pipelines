from airflow import DAG
from operators.get_coinapi_prices_operator import GetCoinAPIPricesOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from datetime import timedelta
import pendulum

schedule_interval = timedelta(days = 1, hours = 2)
start_date = pendulum.datetime(year = 2022,
                               month = 6,
                               day = 8,
                               hour = 2,
                               tz = 'America/Los_Angeles')

with DAG(
    dag_id = 'fetch_token_prices_1h',
    start_date = start_date,
    schedule_interval = schedule_interval,
    max_active_runs =  1,
    catchup = False
) as dag:
    eth_pairs_1h_price_data_to_s3 = GetCoinAPIPricesOperator(
        task_id = 'eth_pairs_1h_price_data_to_s3',
        time_interval = 'hour'
    )

    price_data_1h_cols = ['time_period_start', 'time_period_end', 'time_open', 'time_close',
                          'price_open', 'price_high', 'price_low', 'price_close', 'volume_traded',
                          'trades_count', 'exchange_id', 'symbol_id', 'asset_id_base', 
                          'asset_id_quote']

    s3_eth_pairs_1h_price_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_eth_pairs_1h_price_data_to_redshift',
        schema = 'eth',
        table = 'stg_price_data_1h',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/price_data/coinapi_pair_prices_1_hour.json',
        redshift_conn_id = 'token_price_database_conn',
        aws_conn_id = 's3_conn',
        method = 'UPSERT',
        upsert_keys = ['exchange_id', 'asset_id_base', 'asset_id_quote', 'time_period_start'],
        copy_options = ["json 'auto'", "TIMEFORMAT 'auto'"],
        column_list = price_data_1h_cols
    )

    eth_pairs_1h_price_data_to_s3 >> s3_eth_pairs_1h_price_data_to_redshift
