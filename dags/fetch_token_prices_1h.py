from airflow import DAG
from operators.get_coinapi_prices_operator import GetCoinAPIPricesOperator

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
    dag_id = 'fetch_price_data_1h',
    start_date = start_date,
    schedule_interval = schedule_interval,
    max_active_runs =  1,
    catchup = False
) as dag:
    
    eth_pairs_1h_price_data_to_s3 = GetCoinAPIPricesOperator(
        task_id = 'eth_pairs_1h_price_data_to_s3'
    )

    price_data_1h_cols = [
        'time_period_start', 'time_period_end', 'time_open', 'time_close',
        'price_open', 'price_high', 'price_low', 'price_close', 'volume_traded',
        'trades_count', 'exchange_id', 'asset_id_base', 'asset_id_quote'
    ]

    eth_pairs_1h_price_data_to_s3
