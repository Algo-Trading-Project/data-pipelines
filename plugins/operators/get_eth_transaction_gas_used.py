from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from time import sleep
from web3 import Web3
from time import sleep

import requests as r
import json

# TODO: Implement failure callback function

# TODO: Update gas used start and end block in Airflow
class GetEthTransactionGasUsedOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.s3_connection = S3Hook(aws_conn_id='s3_conn')

        infura_endpoint = 'https://mainnet.infura.io/v3/{}'.format(Variable.get('infura_api_key'))
        eth_node = Web3(Web3.HTTPProvider(infura_endpoint))
        self.web3_instance = eth_node

        self.start_block = int(Variable.get('transaction_gas_used_start_block'))
        self.end_block = int(Variable.get('transaction_gas_used_end_block'))

        self.counter = 0

    ####### HELPER FUNCTIONS ########
    def __update_start_and_end_block_airflow(self, start_block, end_block):
        Variable.set('transaction_gas_used_start_block', start_block)
        Variable.set('transaction_gas_used_end_block', end_block)

    def __upload_to_s3(self, transaction_receipt_data):
        if len(transaction_receipt_data) == 0:
            return

        key = 'eth_data/transaction_gas_used_data/transaction_gas_used.json.{}'.format(self.counter)
        data_to_upload = json.dumps(transaction_receipt_data).replace('[', '').replace(']', '').replace('},', '}')

        self.s3_connection.load_string(
            data_to_upload,
            bucket_name = 'project-poseidon-data',
            key = key,
            replace = True
        )

        self.counter += 1

    def __get_block_transaction_receipts(self, block_num):
        headers = {
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        
        params = {
            'jsonrpc': '2.0',
            'method': 'alchemy_getTransactionReceipts',
            'params':[
                {
                    'blockNumber': '0x' + hex(block_num)[2:].upper()
                }
            ],
            'id': 1
        }

        while True:
            try:
                request_url = 'https://eth-mainnet.g.alchemy.com/v2/{}'.format(Variable.get('alchemy_api_key'))
                response = r.post(
                    url = request_url,
                    headers = headers,
                    json = params
                )
            except Exception as e:
                self.log.error('GetEthTransactionGasUsed: Error getting transaction receipts for block {}'.format(block_num))
                self.log.error('GetEthTransactionGasUsed: {}'.format(str(e)))
                self.log.error('GetEthTransactionGasUsed:')

                return None

            if response.status_code != 200:
                self.log.error('GetEthTransactionGasUsed: Error - {}'.format(response.json()))
                self.log.error('GetEthTransactionGasUsed:')

                return None

            else:
                response = response.json()
                request_result = response.get('result')

                if request_result == None:
                    self.log.error('GetEthTransactionGasUsed: Nothing returned for block {}'.format(block_num))
                    self.log.error('GetEthTransactionGasUsed:')

                    return None
                else:
                    return request_result.get('receipts')
    ####### HELPER FUNCTIONS END ########

    def execute(self, context):
        start_block, end_block = self.start_block, self.end_block
        processed_transaction_receipts = []

        while True:
            if end_block <= start_block:
                self.log.info('GetEthTransactionGasUsed: Reached the beginning of the chain at block {}'.format(start_block))
                self.__upload_to_s3(processed_transaction_receipts)
                return

            if len(processed_transaction_receipts) >= 10000:
                self.log.info('GetEthTransactionGasUsed: Uploading data to S3')
                self.__upload_to_s3(processed_transaction_receipts)
                self.__update_start_and_end_block_airflow(start_block, end_block)
                processed_transaction_receipts = []

            self.log.info(f'GetEthTransactionGasUsed: Processing blocks {end_block} to {start_block}')

            for block_num in range(end_block, start_block - 1, -1):
                transaction_receipts = self.__get_block_transaction_receipts(block_num)

                if transaction_receipts is None:
                    self.log.error(f'GetEthTransactionGasUsed: Error getting transaction receipts for block {block_num}')

                    self.__upload_to_s3(processed_transaction_receipts)
                    self.__update_start_and_end_block_airflow(start_block, block_num)

                    return

                for receipt in transaction_receipts:
                    processed_transaction_receipts.append({
                        'transaction_hash': receipt['transactionHash'].lower(),
                        'block_no': int(receipt['blockNumber'], 16),
                        'effective_gas_price': float(int(receipt['effectiveGasPrice'], 16)) / (10 ** 18),
                        'gas_used': int(receipt['gasUsed'], 16)
                    })
                    
                sleep(0.76)

            end_block = start_block - 1
            start_block = max(end_block - 1000, 0)