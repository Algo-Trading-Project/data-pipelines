from airflow import DAG
from airflow.models import Variable

from operators.web3_alchemy_to_s3_operator import Web3AlchemyToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

import datetime
from airflow.api.client.local_client import Client

with DAG('test', start_date = datetime.datetime.now(), schedule_interval = '@hourly') as dag:
    eth_data_to_s3 = Web3AlchemyToS3Operator(
        task_id = 'get_eth_data',
        batch_size = 1000,
        node_endpoint = Variable.get('infura_endpoint'),
        bucket_name = 'project-poseidon-data',
        region_name = 'us-west-1',
        key = 'eth_data/blocks'
    )
    s3_block_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_block_data_to_redshift',
        schema = 'eth_data',
        table = 'block',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/blocks.json',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        copy_options = ['json']
    )
    s3_transaction_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_transaction_data_to_redshift',
        schema = 'eth_data',
        table = 'transaction',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/transactions.json',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        copy_options = ['json']
    )
    s3_transfer_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_transfer_data_to_redshift',
        schema = 'eth_data',
        table = 'transfer',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/transfers.json',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        copy_options = ['json']
    )
    
    eth_data_to_s3 >> [s3_block_data_to_redshift, s3_transaction_data_to_redshift, s3_transfer_data_to_redshift]