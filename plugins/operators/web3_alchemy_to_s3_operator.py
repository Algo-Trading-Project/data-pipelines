from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.models import Variable
from time import sleep

import requests as r
import json

class Web3AlchemyToS3Operator(BaseOperator):

    def __init__(self, batch_size, node_endpoint, bucket_name, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.batch_size = batch_size

        self.node_endpoint = node_endpoint
        self.web3_instance = None

        self.bucket_name = bucket_name
        self.s3_connection = None

    ############################ HELPER FUNCTIONS ##########################################
    def __set_up_connections(self):
        from web3 import Web3
        
        web_provider = Web3.HTTPProvider(self.node_endpoint)
        self.web3_instance = Web3(web_provider)

        self.s3_connection = S3Hook(
            aws_conn_id = 's3_conn'
        )

    def __get_transfer_data(self):
        start_block = int(Variable.get('start_block'))
        end_block = int(Variable.get('end_block'))
        ############################################ INTERNAL FUNC ############################################
        def get_transfer_data(page_key = None):
            headers = {'Content-Type':'application/json'}

            if page_key == None:
                params = {
                    'jsonrpc':'2.0',
                    'id':0,
                    'method':'alchemy_getAssetTransfers',
                    'params': [{
                        'fromBlock':'0x' + hex(start_block)[2:].upper(),
                        'toBlock':'0x' + hex(end_block)[2:].upper(),
                        'category': [
                            'erc20', 'external', 'internal'
                        ]
                    }]
                }
            else:
                params = {
                    'jsonrpc':'2.0',
                    'id':0,
                    'method':'alchemy_getAssetTransfers',
                    'params': [{
                        'fromBlock':'0x' + hex(start_block)[2:].upper(),
                        'toBlock':'0x' + hex(end_block)[2:].upper(),
                        'category': [
                            'erc20', 'external', 'internal'
                        ],
                        'pageKey':page_key
                    }]
                }

            response = r.post(
                url = Variable.get('alchemy_api_endpoint'),
                headers = headers,
                data = json.dumps(params)
            ).json()

            return response
        ############################################ INTERNAL FUNC END ########################################
        pagination_key = None
    
        preprocessed_transfers = []
        processed_transfers = []

        while True:
            transfer_response = get_transfer_data(pagination_key)
            error = transfer_response.get('error')

            if error != None and error.get('code') == -32604:
                print('one of the api calls to get transfer data timed out... sleeping for 5 minutes and trying again.')
                sleep(60 * 5)
                continue

            preprocessed_transfers.extend(transfer_response['result']['transfers'])

            if transfer_response.get('result').get('pageKey') == None:
                break
            else:
                pagination_key = transfer_response.get('result').get('pageKey')
                continue

        for preprocessed_transfer in preprocessed_transfers:
            processed_transfer = {}
            
            processed_transfer['transaction_hash'] = preprocessed_transfer['hash']
            processed_transfer['block_no'] = int(preprocessed_transfer['blockNum'], 0)
            processed_transfer['from_'] = preprocessed_transfer['from']
            processed_transfer['to_'] = preprocessed_transfer['to']
            processed_transfer['token_units'] = str(preprocessed_transfer['value'])
            processed_transfer['raw_token_units'] = str(preprocessed_transfer['rawContract']['value'])
            processed_transfer['asset'] = preprocessed_transfer['asset']
            processed_transfer['category'] = preprocessed_transfer['category']
            processed_transfer['token_address'] = preprocessed_transfer['rawContract']['address']

            processed_transfers.append(processed_transfer)

        print()
        print('{} transfers found between block {} and {}'.format(len(processed_transfers), start_block, end_block))
        print()

        return processed_transfers

    def __get_block_data(self):
        from web3.exceptions import BlockNotFound

        start_block = int(Variable.get('start_block'))
        end_block = int(Variable.get('end_block'))

        print()
        print('fetching data between blocks {} and {} ({} total)'.format(start_block, end_block, end_block - start_block))
        print()
        
        transaction_batch = []
        block_batch = []
        preprocessed_blocks = []

        while True:
            try:
                preprocessed_blocks = [self.web3_instance.eth.get_block(i, full_transactions = True) for i in range(start_block, end_block + 1)]
                break
            except BlockNotFound:
                print()
                print('One of the blocks could not be found... Sleeping for 5 minutes and trying again.')
                print()
                sleep(60 * 5)
                continue
                
        for block in preprocessed_blocks:
            processed_block_data = {}
            
            processed_block_data['block_no'] = int(block['number'])
            processed_block_data['hash'] = block['hash'].hex()
            processed_block_data['parent_hash'] = block['parentHash'].hex()
            processed_block_data['timestamp'] = int(block['timestamp'])
            processed_block_data['difficulty'] = int(block['difficulty'])
            processed_block_data['miner'] = block['miner']
            processed_block_data['gas_used'] = int(block['gasUsed'])
            processed_block_data['size'] = int(block['size'])
            processed_block_data['sha3_uncles'] = block['sha3Uncles'].hex()
            processed_block_data['gas_limit'] = int(block['gasLimit'])

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

        start_block = int(Variable.get('start_block'))
        end_block = int(Variable.get('end_block'))

        for transaction in transaction_batch:
            preprocessed_transactions.extend(transaction)

        print()
        print('{} transactions found between block {} and {}'.format(len(preprocessed_transactions), start_block, end_block))
        print()

        for transaction in preprocessed_transactions:
            processed_transaction = {}

            processed_transaction['transaction_hash'] = transaction['hash'].hex()
            processed_transaction['block_no'] = int(transaction['blockNumber'])
            processed_transaction['to_'] = transaction['to']
            processed_transaction['from_'] = transaction['from']
            # value is converted from wei to ether units before being stored to
            # prevent too large of a number from being stored
            processed_transaction['value'] = int(transaction['value']) / (10 ** 18)
            processed_transaction['gas'] = int(transaction['gas'])
            processed_transaction['gas_price'] = int(transaction['gasPrice'])

            # processed_transaction['transaction_method'] = self.__get_transaction_method(processed_transaction)
            
            processed_transactions.append(processed_transaction)
            
        return processed_transactions

    def __upload_to_s3(self, block_data, transaction_data, transfer_data):
        json_block_data = json.dumps(block_data).replace('[', '').replace(']', '').replace('},', '}')
        self.s3_connection.load_string(
            json_block_data,
            key = 'eth_data/blocks.json',
            bucket_name = self.bucket_name,
            replace = True
        )

        json_transaction_data = json.dumps(transaction_data).replace('[', '').replace(']', '').replace('},', '}')
        self.s3_connection.load_string(
            json_transaction_data,
            key = 'eth_data/transactions.json',
            bucket_name = self.bucket_name,
            replace = True
        )

        json_transfer_data = json.dumps(transfer_data).replace('[', '').replace(']', '').replace('},', '}')
        self.s3_connection.load_string(
            json_transfer_data,
            key = 'eth_data/transfers.json',
            bucket_name = self.bucket_name,
            replace = True
        )

    ############################ HELPER FUNCTIONS END ######################################

    def execute(self, context):
        print()
        print('start block: {}'.format(Variable.get(key = 'start_block')))
        print('end block: {}'.format(Variable.get(key = 'end_block')))
        print()
        self.__set_up_connections()
       
        block_data, transaction_batch = self.__get_block_data()
        transaction_data = self.__get_transaction_data(transaction_batch)
        transfer_data = self.__get_transfer_data()
        
        self.__upload_to_s3(block_data, transaction_data, transfer_data)

        end_block = int(Variable.get('end_block'))

        if self.batch_size == None:
            new_start_block = min(self.web3_instance.eth.block_number, end_block + 1)
            new_end_block = self.web3_instance.eth.block_number
        else:
            new_start_block = min(self.web3_instance.eth.block_number, end_block + 1)
            new_end_block = new_start_block + self.batch_size

        Variable.set(key = 'start_block', value = new_start_block)
        Variable.set(key = 'end_block', value = new_end_block)

        print()
        print('new start block: {}'.format(new_start_block))
        print('new end block: {}'.format(new_end_block))