from airflow import DAG
from airflow.models import Variable
from operators.web3_alchemy_to_s3_operator import Web3AlchemyToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.state import State
from airflow.operators.dummy import DummyOperator

from datetime import timedelta
import pendulum

schedule_interval = timedelta(minutes = 15)
start_date = pendulum.datetime(year = 2022,
                               month = 3,
                               day = 26,
                               hour = 5,
                               tz = 'America/Los_Angeles')

batch_size_map = {1:2000, 2:2000, 3:2000, 4:2000}

for i in range(1, 5):
    dag_id = 'fetch_historical_eth_data_{}'.format(i)
    dag = DAG(dag_id,
              start_date = start_date, 
              schedule_interval = schedule_interval)
    with dag:
        previous_dag_run_sensor = ExternalTaskSensor(
            task_id = 'check_previous_dag_run_finished',
            external_dag_id = dag_id,
            external_task_id = 'dag_run_finished',
            allowed_states = [State.SUCCESS],
            failed_states = [State.FAILED],
            execution_delta = schedule_interval
        )

        eth_data_to_s3 = Web3AlchemyToS3Operator(
            task_id = 'get_historical_eth_data',
            batch_size = batch_size_map[i],
            node_endpoint = Variable.get('infura_endpoint'),
            bucket_name = 'project-poseidon-data',
            is_historical_run = True,
            start_block_variable_name = 'start_block_{}'.format(i),
            end_block_variable_name = 'end_block_{}'.format(i)
        )     

        s3_key_prefix = 'eth_data_historical/start_block_{}-end_block_{}'.format(i, i)

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
            s3_key = '{}/blocks.json'.format(s3_key_prefix),
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
            s3_key = '{}/transactions.json'.format(s3_key_prefix),
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
            s3_key = '{}/transfers.json'.format(s3_key_prefix),
            redshift_conn_id = 'redshift_conn',
            aws_conn_id = 's3_conn',
            column_list = transfer_event_table_cols,
            copy_options = ["json 'auto'"]
        )

        finish = DummyOperator(
            task_id = 'dag_run_finished'
        )

        previous_dag_run_sensor >> eth_data_to_s3 >> [s3_block_data_to_redshift, s3_transaction_data_to_redshift, s3_transfer_data_to_redshift] >> finish
    
    globals()[dag_id] = dag
    