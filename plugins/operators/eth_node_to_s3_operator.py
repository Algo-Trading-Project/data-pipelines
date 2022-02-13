from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.models import Variable

from web3 import Web3

import requests as r

class EthNodeToS3Operator(BaseOperator):

    def __init__(self, batch_size, node_endpoint, bucket_name, region_name, key, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.batch_size = batch_size

        self.node_endpoint = node_endpoint
        self.web3_instance = None

        self.bucket_name = bucket_name
        self.region_name = region_name
        self.key = key
        self.s3_connection = None

    ############################ HELPER FUNCTIONS ##########################################

    ############################ HELPER FUNCTIONS END ######################################

    def execute(self, context):
        web_provider = Web3.HTTPProvider(self.node_endpoint)
        self.web3_instance = Web3(web_provider)
        
        aws_credentials = Variable.get('aws_credentials', deserialize_json = True)
        self.s3_connection = S3Hook(
            aws_credentials['aws_access_key_id'],
            aws_credentials['aws_secret_access_key']
        )

        # params = {
        #     'jsonrpc':'2.0',
        #     'id':0,
        #     'method':'alchemy_getAssetTransfers',
        #     'params': [{
        #         'fromBlock':'0x' + hex(int(Variable.get('start_block'))[2:].upper()),
        #         'toBlock':'0x' + hex(int(Variable.get('end_block'))[2:].upper()),
        #         'category': [
        #             'erc20', 'external', 'internal'
        #         ]
        #     }]
        # }
        # headers = {'Content-Type':'application/json'}

        # response = r.get(
        #     url = Variable.get('alchemy_api_endpoint'),
        #     headers = headers,
        #     params = params
        # )

        # return response



        
