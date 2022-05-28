from airflow import DAG
from airflow.models import Variable

from operators.get_coinapi_prices_operator import GetCoinAPIPricesOperator

from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta
import pendulum


schedule_interval = timedelta(days = 1)
start_date = pendulum.datetime(year = 2022,
                               month = 5,
                               day = 28,
                               hour = 7,
                               tz = 'America/Los_Angeles')

with DAG(
    dag_id = 'fetch_coinapi_eth_pair_token_prices',
    schedule_interval = schedule_interval,
    start_date = start_date,
    catchup = False,
    max_active_runs = 1
) as dag:
    test = GetCoinAPIPricesOperator(task_id = 's', time_interval = 'day')

    test
