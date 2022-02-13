from airflow import DAG
from airflow.models import Variable
from operators.eth_node_to_s3_operator import EthNodeToS3Operator
import datetime

with DAG('test') as dag:
    t1 = EthNodeToS3Operator(
        task_id = '1',
        start_date = datetime.datetime.now(),
        batch_size = 1000,
        node_endpoint = Variable.get('infura_endpoint'),
        bucket_name = 'project-poseidon-data',
        region_name = 'us-west-1',
        key = 'eth_data/blocks'
    )
    t1.execute({})

    # sudo lsof -i tcp:8080
    # kill -9 408103312\\
