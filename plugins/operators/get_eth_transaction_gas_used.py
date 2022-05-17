from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

import requests as r
import json

class GetEthTransactionGasUsedOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.s3_connection = None

    ####### HELPER FUNCTIONS #######
    def __get_block_transaction_receipts(self, block_num):
        headers = {'Content-Type': 'application/json'}
        try:
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

            response = r.post(
                url = Variable.get('alchemy_api_endpoint'),
                headers = headers,
                data = json.dumps(params)
            ).json()

            print(response)

            request_result = response.get('result')
            
            if request_result == None:
                return []
            
            request_receipts = request_result.get('receipts')

            if request_receipts == None:
                return []

            return request_receipts

        except Exception as e:
            print(e)

    def __upload_to_s3(self, transaction_receipt_data):
        self.s3_connection = S3Hook(aws_conn_id = 's3_conn')

        data_to_upload = json.dumps(transaction_receipt_data).replace('[', '').replace(']', '').replace('},', '}')

        self.s3_connection.load_string(
            data_to_upload,
            bucket_name = 'project-poseidon-data',
            key = 'eth_data/transaction_gas_used_data/transaction_gas_used.json',
            replace = True
        )

    def execute(self, context):
        start_block = int(Variable.get('transaction_gas_used_start_block'))
        end_block = int(Variable.get('transaction_gas_used_end_block'))

        preprocessed_transaction_receipts = []
        processed_transaction_receipts = []

        for block_num in range(start_block, end_block + 1):
            transaction_recipts = self.__get_block_transaction_receipts(block_num)
            preprocessed_transaction_receipts.extend(transaction_recipts)

        for preprocesseced_transaction_receipt in preprocessed_transaction_receipts:
            processed_transaction_receipt = {}

            processed_transaction_receipt['transaction_hash'] = preprocesseced_transaction_receipt['transactionHash'].lower()
            processed_transaction_receipt['block_no'] = int(preprocesseced_transaction_receipt['blockNumber'], 16)
            # value is converted from wei to ether units before being stored to
            # prevent too large of a number from being stored
            processed_transaction_receipt['gas_used'] = int(preprocesseced_transaction_receipt['gasUsed'], 16) / (10 ** 18)

            processed_transaction_receipts.append(processed_transaction_receipt)

        self.__upload_to_s3(processed_transaction_receipts)