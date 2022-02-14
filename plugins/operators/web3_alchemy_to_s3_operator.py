from distutils.command.config import config
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.models import Variable
from botocore.config import Config

from web3 import Web3

import requests as r
import json

class Web3AlchemyToS3Operator(BaseOperator):

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
    def __set_up_connections(self):
        web_provider = Web3.HTTPProvider(self.node_endpoint)
        self.web3_instance = Web3(web_provider)

        self.s3_connection = S3Hook(
            aws_conn_id = 's3_conn'
        )

    def __get_transfer_data(self):
        params = {
            'jsonrpc':'2.0',
            'id':0,
            'method':'alchemy_getAssetTransfers',
            'params': [{
                'fromBlock':'0x' + hex(int(Variable.get('start_block')))[2:].upper(),
                'toBlock':'0x' + hex(int(Variable.get('end_block')))[2:].upper(),
                'category': [
                    'erc20', 'external', 'internal'
                ]
            }]
        }
        headers = {'Content-Type':'application/json'}

        response = r.post(
            url = Variable.get('alchemy_api_endpoint'),
            headers = headers,
            data = json.dumps(params)
        )
        
        preprocessed_transfers = response.json()['result']['transfers']
        processed_transfers = []

        for preprocessed_transfer in preprocessed_transfers:
            processed_transfer = {}
            
            processed_transfer['transaction_hash'] = preprocessed_transfer['hash']
            processed_transfer['block_no'] = int(preprocessed_transfer['blockNum'], 0)
            processed_transfer['from'] = preprocessed_transfer['from']
            processed_transfer['to'] = preprocessed_transfer['to']
            processed_transfer['token_units'] = preprocessed_transfer['value']
            processed_transfer['raw_token_units'] = int(preprocessed_transfer['rawContract']['value'], 0)
            processed_transfer['asset'] = preprocessed_transfer['asset']
            processed_transfer['category'] = preprocessed_transfer['category']
            processed_transfer['token_address'] = preprocessed_transfer['rawContract']['address']

            processed_transfers.append(processed_transfer)

        return processed_transfers

    def __get_block_data(self):
        start_block = int(Variable.get('start_block'))
        end_block = int(Variable.get('end_block'))
        
        transaction_batch = []
        block_batch = []

        preprocessed_blocks = [self.web3_instance.eth.get_block(i, full_transactions = True) for i in range(start_block, end_block + 1)]
        
        for block in preprocessed_blocks:
            processed_block_data = {}
            
            processed_block_data['block_no'] = block['number']
            processed_block_data['hash'] = block['hash'].hex()
            processed_block_data['parent_hash'] = block['parentHash'].hex()
            processed_block_data['timestamp'] = block['timestamp']
            processed_block_data['difficulty'] = block['difficulty']
            processed_block_data['miner'] = block['miner']
            processed_block_data['gas_used'] = block['gasUsed']
            processed_block_data['size'] = block['size']
            processed_block_data['sha3uncles'] = block['sha3Uncles'].hex()
            processed_block_data['gas_limit'] = block['gasLimit']

            block_batch.append(processed_block_data)
            transaction_batch.append(block['transactions'])

        return block_batch, transaction_batch

    def __get_transaction_method(self, transaction):
        if self.web3_instance.eth.get_code(transaction['to']) == '0x':
            return 'Transfer'

        tx = self.web3_instance.eth.get_transaction(transaction['transaction_hash'])
        etherscan_api_key = Variable.get('etherscan_api_key')
        abi_endpoint = "https://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}".format(transaction['to'], etherscan_api_key)
        
        abi = json.loads(r.get(abi_endpoint).text)
        try:
            contract = self.web3_instance.eth.contract(address = transaction['to'], abi = abi["result"])
            func_obj, func_params = contract.decode_function_input(tx["input"])
            return func_obj.functions.name().call()
        except Exception as e:
            return None

    def __get_transaction_data(self, transaction_batch):
        preprocessed_transactions = []
        processed_transactions = []

        for transaction in transaction_batch:
            preprocessed_transactions.extend(transaction)

        for transaction in preprocessed_transactions:
            processed_transaction = {}

            processed_transaction['transaction_hash'] = transaction['hash'].hex()
            processed_transaction['block_no'] = transaction['blockNumber']
            processed_transaction['to'] = transaction['to']
            processed_transaction['from'] = transaction['from']
            processed_transaction['value'] = transaction['value']
            processed_transaction['gas'] = transaction['gas']
            processed_transaction['gas_price'] = transaction['gasPrice']

            # processed_transaction['transaction_method'] = self.__get_transaction_method(processed_transaction)
            
            processed_transactions.append(processed_transaction)
            
        return processed_transactions

    def __upload_to_s3(self, block_data, transaction_data, transfer_data):
        self.s3_connection.load_string(
            json.dumps(block_data),
            key = 'eth_data/blocks.json',
            bucket_name = self.bucket_name,
            replace = True
        )

        self.s3_connection.load_string(
            json.dumps(transaction_data),
            key = 'eth_data/transactions.json',
            bucket_name = self.bucket_name,
            replace = True
        )

        self.s3_connection.load_string(
            json.dumps(transfer_data),
            key = 'eth_data/transfers.json',
            bucket_name = self.bucket_name,
            replace = True
        )

    ############################ HELPER FUNCTIONS END ######################################

    def execute(self, context):
        self.__set_up_connections()
       
        block_data, transaction_batch = self.__get_block_data()
        transaction_data = self.__get_transaction_data(transaction_batch)
        transfer_data = self.__get_transfer_data()
        
        self.__upload_to_s3(block_data, transaction_data, transfer_data)
        # Variable.set(
        #     key = 'start_block',
        #     value = str(int(Variable.get('end_block')) + 1)
        # )
        # Variable.set(
        #     key = 'end_block',
        #     value = str(self.web3_instance.eth.block_number)
        # )