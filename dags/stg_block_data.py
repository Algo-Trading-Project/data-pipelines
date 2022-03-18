from airflow import DAG
from airflow.models import Variable
from operators.web3_alchemy_to_s3_operator import Web3AlchemyToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from datetime import timedelta
import pendulum

# timezone = pytz.timezone('America/Los_Angeles')
schedule_interval = timedelta(minutes = 15)
start_date = pendulum.datetime(year = 2022, month = 3, day = 18, hour = 10, tz = 'America/Los_Angeles')

with DAG('get_eth_block_and_transaction_data',
          start_date = start_date, 
          schedule_interval = schedule_interval) as dag:

    eth_data_to_s3 = Web3AlchemyToS3Operator(
        task_id = 'get_eth_data',
        batch_size = 300,
        node_endpoint = Variable.get('infura_endpoint'),
        bucket_name = 'project-poseidon-data',
        key = 'eth_data',
        region_name = 'us-west-2'
    )     

    block_table_cols = ['block_no', 'block_hash', 'parent_hash', 'timestamp',
                        'difficulty', 'miner_address', 'gas_used',
                        'block_size', 'sha3_uncles', 'gas_limit']
    s3_block_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_eth_block_data_to_redshift',
        schema = 'eth_data',
        table = 'block',
        method = 'UPSERT',
        upsert_keys = ['block_no'],
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/blocks.json',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        column_list = block_table_cols,
        copy_options = ["json 'auto'"]
    )

    transaction_table_cols = ['transaction_hash', 'block_no', 'to_',
                              'from_', 'value', 'gas', 'gas_price']
    s3_transaction_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_eth_transaction_data_to_redshift',
        schema = 'eth_data',
        table = 'transaction',
        method = 'UPSERT',
        upsert_keys = ['transaction_hash'],
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/transactions.json',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        column_list = transaction_table_cols,
        copy_options = ["json 'auto'"]
    )

    transfer_event_table_cols = ['transaction_hash', 'block_no', 'from_',
                                 'to_', 'token_units', 'raw_token_units',
                                 'asset', 'transfer_category', 'token_address']
    s3_transfer_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_eth_transfer_data_to_redshift',
        schema = 'eth_data',
        table = 'transfer',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/transfers.json',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        column_list = transfer_event_table_cols,
        copy_options = ["json 'auto'"]
    )

    eth_data_to_s3 >> [s3_block_data_to_redshift, s3_transaction_data_to_redshift, s3_transfer_data_to_redshift]