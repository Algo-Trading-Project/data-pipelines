from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

import requests as r
import json

class GetBlockRewardsOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.s3_connection = None

    def __get_start_and_end_block(self):
        start_block = int(Variable.get('block_rewards_start_block'))
        end_block = int(Variable.get('block_rewards_end_block'))

        return start_block, end_block

    def __get_block_rewards_data(self, block):
        block_rewards_data = []

        api_url = 'https://api.etherscan.io/api?module=block&action=getblockreward&blockno={}&apikey={}'.format(block, Variable.get('etherscan_api_key'))
        response = r.get(api_url).json()
        result = response['result']

        block_rewards_data.append({
            'block_no':int(result['blockNumber']), 
            'miner_address':result['blockMiner'].lower(),
            # Convert wei units into ether units
            'block_reward':(int(result['blockReward']) + int(result['uncleInclusionReward'])) / (10 ** 18)
        })

        for uncle in result['uncles']:
            block_rewards_data.append({
                'block_no':int(result['blockNumber']),
                'miner_address':uncle['miner'].lower(),
                # Convert wei units into ether units
                'block_reward':int(uncle['blockreward']) / (10 ** 18)
            })

        return block_rewards_data

    def __upload_to_s3(self, block_rewards_data):
        self.s3_connection = S3Hook(aws_conn_id = 's3_conn')

        data_to_upload = json.dumps(block_rewards_data).replace('[', '').replace(']', '').replace('},', '}')

        self.s3_connection.load_string(
            data_to_upload,
            bucket_name = 'project-poseidon-data',
            key = 'eth_data/block_rewards_data/block_rewards.json',
            replace = True
        )

    def execute(self, context):
        start_block, end_block = self.__get_start_and_end_block()

        processed_block_rewards = []

        for block in range(start_block, end_block + 1):
            block_rewards_data = self.__get_block_rewards_data(block)
            processed_block_rewards.extend(block_rewards_data)

        self.__upload_to_s3(processed_block_rewards)
