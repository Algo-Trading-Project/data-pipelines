from airflow.models import BaseOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

import requests as r
import json
import pandas as pd

class GetCoinAPIPricesOperator(BaseOperator):

    def __init__(self, time_interval, **kwargs):
        super().__init__(**kwargs)

        self.time_interval = time_interval        
        self.s3_connection = S3Hook(aws_conn_id = 's3_conn')

    def execute(self, context):
        key = 'eth_data/price_data/coinapi_eth_pairs.json'
        
        coinapi_eth_pairs_str = self.s3_connection.read_key(key = key, bucket_name = 'project-poseidon-data')
        coinapi_eth_pairs_json = json.loads(coinapi_eth_pairs_str)

        print(coinapi_eth_pairs_json)

        df = pd.read_json(coinapi_eth_pairs_json, orient = 'records')
        print(df)