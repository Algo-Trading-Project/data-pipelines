from airflow import DAG
from airflow.models import Variable

from operators.get_eth_transaction_gas_used import GetEthTransactionGasUsedOperator

from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta
import pendulum

schedule_interval = timedelta(days = 1, hours = 1)
start_date = pendulum.datetime(year = 2022,
                               month = 4,
                               day = 14,
                               hour = 1,
                               tz = 'America/Los_Angeles')

with DAG(
    dag_id = 'fetch_eth_transaction_gas_used',
    schedule_interval = schedule_interval,
    start_date = start_date,
    catchup = False,
    max_active_runs = 1
) as dag:

    gas_used_to_s3 = GetEthTransactionGasUsedOperator(
        task_id = 'get_eth_transaction_gas_used'
    )

    gas_used_table_cols = ['transaction_hash', 'block_no', 'gas_used', 'effective_gas_price']
    s3_gas_used_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_transaction_gas_used_to_redshift',
        schema = 'eth_data',
        table = 'gas_used',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/transaction_gas_used_data',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        column_list = gas_used_table_cols,
        copy_options = ["json 'auto'"],
        method = 'UPSERT',
        upsert_keys = ['transaction_hash', 'block_no']
    )

    gas_used_to_s3 >> s3_gas_used_to_redshift