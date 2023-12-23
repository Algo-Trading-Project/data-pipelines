from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from web3 import Web3

import requests as r
import json

# TODO: Implement failure callback function

# TODO: Update block rewards start and end block in Airflow

# TODO: Improve error handling
class GetBlockRewardsOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.s3_connection = S3Hook(aws_conn_id = 's3_conn')

        self.start_block = int(Variable.get('block_rewards_start_block'))
        self.end_block = int(Variable.get('block_rewards_end_block'))

        infura_endpoint = 'https://mainnet.infura.io/v3/{}'.format(Variable.get('infura_api_key'))
        eth_node = Web3(Web3.HTTPProvider(infura_endpoint))
        self.web3_instance = eth_node
        
        self.counter = 0

    def update_start_and_end_block_airflow(self, start_block, end_block):
        Variable.set('block_rewards_start_block', start_block)
        Variable.set('block_rewards_end_block', end_block)

    def __upload_to_s3(self, block_rewards_data):
        if len(block_rewards_data) == 0:
            return

        key = 'eth_data/block_rewards_data/block_rewards.json.{}'.format(self.counter)
        data_to_upload = json.dumps(block_rewards_data).replace('[', '').replace(']', '').replace('},', '}')

        self.s3_connection.load_string(
            data_to_upload,
            bucket_name = 'project-poseidon-data',
            key = key,
            replace = True
        )

        self.counter += 1

    def __get_block_rewards_data(self, block):
        block_rewards_data = []

        try:
            api_url = 'https://api.etherscan.io/api?module=block&action=getblockreward&blockno={}&apikey={}'.format(block, Variable.get('etherscan_api_key'))
            response = r.get(api_url).json()
        except Exception as e:
            self.log.error('GetBlockRewards: Error getting block rewards data for block {}'.format(block))
            self.log.error('GetBlockRewards: {}'.format(e))
            self.log.error('GetBlockRewards:')

            return None

        success = int(response['status'])

        if success:
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

        else:
            result = response['result']
            self.log.error('GetBlockRewawrds: ERROR - {}'.format(result))
            self.log.error('GetBlockRewards:')

            return None

    def execute(self, context):
        start_block, end_block = self.start_block, self.end_block
        processed_block_rewards = []

        while True:

            if start_block >= end_block:
                self.log.info('GetBlockRewards: Stopping at block {}'.format(start_block))
                self.log.info('GetBlockRewards:')

                end_block = min(start_block, self.web3_instance.eth.block_number)

                self.log.info('GetBlockRewards: Updating start and end block in Airflow')
                self.log.info('GetBlockRewards: New start block - {}'.format(start_block))
                self.log.info('GetBlockRewards: New end block - {}'.format(end_block))
                self.log.info('GetBlockRewards:')

                self.__upload_to_s3(processed_block_rewards)
                self.update_start_and_end_block_airflow(start_block, end_block)

                return

            if len(processed_block_rewards) >= 10000:
                self.log.info('GetBlockRewards: At least 10,000 elements collected. Uploading current data to S3')
                self.log.info('GetBlockRewards:')

                self.__upload_to_s3(processed_block_rewards)
                self.update_start_and_end_block_airflow(start_block, end_block)

                processed_block_rewards = []

            self.log.info('GetBlockRewards: Processing blocks {} to {}'.format(start_block, end_block))
            self.log.info('GetBlockRewards:')

            for block in range(start_block, end_block + 1):
                block_rewards_data = self.__get_block_rewards_data(block)

                if block_rewards_data is None:
                    self.log.error('GetBlockRewards: Error getting block rewards data for block {}'.format(block))
                    self.log.error('GetBlockRewards: New start block - {}'.format(block))
                    self.log.error('GetBlockRewards: New end block - {}'.format(end_block))
                    self.log.error('GetBlockRewards:')

                    self.__upload_to_s3(processed_block_rewards)
                    self.update_start_and_end_block_airflow(block, end_block)
                    
                    return
            
                processed_block_rewards.extend(block_rewards_data)

            start_block = end_block + 1
            end_block = min(start_block + 1000, self.web3_instance.eth.block_number)

