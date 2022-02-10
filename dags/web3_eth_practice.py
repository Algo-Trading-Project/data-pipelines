import re
import requests
import json

from web3 import Web3
from web3.logs import DISCARD

class TransactionScraper:

    def __init__(self, etherscan_api_key, infura_endpoint = None):
        self.etherscan_api_key = etherscan_api_key
        
        provider = Web3.HTTPProvider(endpoint_uri = infura_endpoint)
        self.web3_instance = Web3(provider = provider)

    ############################## HELPER FUNCTIONS ##################################
    def __get_contract(self, address):    
        contract_abi_endpoint = ('https://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'
                                .format(address, self.etherscan_api_key)
                                )

        response = requests.get(url = contract_abi_endpoint).text
        response = json.loads(response)

        if response['status'] == '1':
            contract = self.web3_instance.eth.contract(
                address = address,
                abi = response['result']
            )

            return contract

    def __get_contract_abi_events(self, contract, filter_for_event):
        events = [x for x in contract.abi if (x['type'] == 'event') and (x['name'] == filter_for_event)]
        return events

    def __decode_log(self, contract_events, event_signature_hash_for_log, contract, tx_receipt):
        event_signature_hash_for_log = Web3.toHex(event_signature_hash_for_log)

        for event in contract_events:
            event_name = event['name']
            event_inputs = [input['type'] for input in event['inputs']]
            event_signature = '{}({})'.format(event_name, ','.join(event_inputs))
            
            hashed_event_signature = Web3.toHex(Web3.keccak(text = event_signature))

            if hashed_event_signature == event_signature_hash_for_log:
                decoded_log = contract.events[event_name]().processReceipt(tx_receipt, errors = DISCARD)
                return decoded_log
        
        return ()
    ############################## HELPER FUNCTIONS END ##############################

    def decode_logs_from_transaction(self, tx_hash, filter_for_event = 'Transfer'):
        previously_seen_signature_hashes = {}

        transaction_receipt = (
            self.web3_instance
            .eth
            .get_transaction_receipt(tx_hash)
        )
        transaction_logs = transaction_receipt['logs']
        
        decoded_logs = []

        for log in transaction_logs:
            contract = self.__get_contract(address = log['address'])
            
            if contract == None:
                continue

            event_signature_hash = log['topics'][0]
            contract_events = self.__get_contract_abi_events(contract = contract, filter_for_event = filter_for_event)

            if previously_seen_signature_hashes.get(event_signature_hash) == None:            
                decoded_info = self.__decode_log(
                    contract_events,
                    event_signature_hash,
                    contract,
                    transaction_receipt
                )
                
                for decoded_log in decoded_info: 
                    decoded_logs.append(decoded_log)

                previously_seen_signature_hashes[event_signature_hash] = True

        return decoded_logs

    def get_internal_txs_for_tx(self, tx_hash):
        request_url = 'https://api.etherscan.io/api?module=account&action=txlistinternal&txhash={}&apikey={}'.format(tx_hash, self.etherscan_api_key)

        response = requests.get(request_url).text
        return json.loads(response)

infura_endpoint = 'https://mainnet.infura.io/v3/697f7138e01947fe909c0b438b74a261'
etherscan_api_key = 'WUYJ9F4ESXW3IA33W231RMQ8BJK5C9G464'

tx_scraper = TransactionScraper(etherscan_api_key = etherscan_api_key,
                                infura_endpoint = infura_endpoint)

tx_hash = '0x8d5f3c06d0931be19a73290548d5e01161690ff03143b2297bf59d343e59148a'

internal_txs = tx_scraper.get_internal_txs_for_tx(tx_hash = tx_hash)
internal_events = tx_scraper.decode_logs_from_transaction(tx_hash = tx_hash)

print()
print(internal_txs)

# for tx in internal_events:
#     print()
#     print(tx)
#     print()
