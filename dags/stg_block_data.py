from airflow import DAG
from airflow.models import Variable

from operators.web3_alchemy_to_s3_operator import Web3AlchemyToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

import datetime
from airflow.api.client.local_client import Client

with DAG('test', start_date = datetime.datetime.now(), schedule_interval = '@hourly[') as dag:
    eth_data_to_s3 = Web3AlchemyToS3Operator(
        task_id = 'get_eth_data',
        batch_size = 1000,
        node_endpoint = Variable.get('infura_endpoint'),
        bucket_name = 'project-poseidon-data',
        region_name = 'us-west-2',
        key = 'eth_data/blocks'
    )     

    block_table_cols = ['block_no', 'hash', 'parent_hash', 'timestamp',
                        'difficulty', 'miner', 'gas_used',
                        'size', 'sha3_uncles', 'gas_limit']
    s3_block_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_block_data_to_redshift',
        schema = 'eth_data',
        table = 'block',
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
        task_id = 's3_transaction_data_to_redshift',
        schema = 'eth_data',
        table = 'transaction',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/transactions.json',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        column_list = transaction_table_cols,
        copy_options = ["json 'auto'"]
    )

    transfer_event_table_cols = ['transaction_hash', 'block_no', 'from_',
                                 'to_', 'token_units', 'raw_token_units',
                                 'asset', 'category', 'token_address']
    s3_transfer_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_transfer_data_to_redshift',
        schema = 'eth_data',
        table = 'transfer_event',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/transfers.json',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        column_list = transfer_event_table_cols,
        copy_options = ["json 'auto'"]
    )

    eth_data_to_s3 >> [s3_block_data_to_redshift, s3_transaction_data_to_redshift, s3_transfer_data_to_redshift]
