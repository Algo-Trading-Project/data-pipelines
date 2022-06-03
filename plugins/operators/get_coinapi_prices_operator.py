from airflow.models import BaseOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

import requests as r
import json
import pandas as pd
import dateutil.parser as parser

class GetCoinAPIPricesOperator(BaseOperator):

    def __init__(self, time_interval, **kwargs):
        super().__init__(**kwargs)

        self.time_interval = time_interval        
        self.s3_connection = S3Hook(aws_conn_id = 's3_conn')

    def __get_next_start_date(self, eth_pair):
        most_recent_data_date = eth_pair['latest_scrape_date_1_{}'.format(self.time_interval)]
        next_start_date = None

        if pd.isnull(most_recent_data_date):
            next_start_date = parser.parse(str(eth_pair['data_start'])).isoformat().split('+')[0]
        else:
            next_start_date = parser.parse(str(most_recent_data_date)).isoformat().split('+')[0]

        return next_start_date

    def __upload_new_price_data(self, new_price_data):
        data_to_uplaod = json.dumps(new_price_data).replace('[', '').replace(']', '').replace('},', '}')
        key = 'eth_data/price_data/coinapi_pair_prices_1_{}.json'.format(self.time_interval)

        self.s3_connection.load_string(
            string_data = data_to_uplaod, 
            key = key, 
            bucket_name = 'project-poseidon-data', 
            replace = True
        )

    def __update_coinapi_eth_pairs_metadata(self, latest_price_data_for_pair, coinapi_eth_pairs_df, exchange_id, asset_id_base, asset_id_quote):
        sort_key = lambda x: pd.to_datetime(x['time_period_start'])

        element_w_latest_date = max(latest_price_data_for_pair, key = sort_key)
        new_latest_scrape_date = parser.parse(element_w_latest_date['time_period_start']).isoformat()

        predicate = (coinapi_eth_pairs_df['exchange_id'] == exchange_id) & (coinapi_eth_pairs_df['asset_id_base'] == asset_id_base) & (coinapi_eth_pairs_df['asset_id_quote'] == asset_id_quote)
        
        coinapi_eth_pairs_df.loc[predicate, 'latest_scrape_date_1_{}'.format(self.time_interval)] = new_latest_scrape_date
        coinapi_eth_pairs_json = coinapi_eth_pairs_df.to_dict(orient = 'records')
        coinapi_eth_pairs_str = json.dumps(coinapi_eth_pairs_json).replace('[', '').replace(']', '').replace('},', '}')

        self.s3_connection.load_string(
            string_data = coinapi_eth_pairs_str,
            key = 'eth_data/price_data/coinapi_pair_metadata.json',
            bucket_name = 'project-poseidon-data',
            replace = True
        )

    def __get_latest_price_data(self, coinapi_symbol_id, period_id, time_start):
        
        def format_response_data(response):
            exchange_id, symbol_id, asset_id_base, asset_id_quote = coinapi_symbol_id.split('_')
            
            for elem in response:
                elem['exchange_id'] = exchange_id
                elem['symbol_id'] = symbol_id
                elem['asset_id_base'] = asset_id_base
                elem['asset_id_quote'] = asset_id_quote

            return response

        api_request_url = 'https://rest.coinapi.io/v1/ohlcv/{}/history?period_id={}&time_start={}&limit={}'.format(coinapi_symbol_id, period_id, time_start, 100000)
        headers = {'X-CoinAPI-Key':Variable.get('coin_api_api_key')}
        
        response = r.get(
            url = api_request_url,
            headers = headers,
        )

        response_json = response.json()

        print('api request url: {}'.format(api_request_url))
        print()

        # Error occurred during request; Exceeded API key's 24 hour requests executed limit
        if type(response_json) == dict and response.status_code == 429:
            print(response_json)
            return 429

        # Request returned no data
        elif type(response_json) == list and len(response_json) == 0:
            print(response_json)
            return None

        elif 'error' in response_json:
            print(response_json)
            return None

        formatted_response = format_response_data(response_json)

        return formatted_response

    def execute(self, context):
        period_id_map = {'min':'1MIN', 'hour':'1HRS', 'day':'1DAY'}
        key = 'eth_data/price_data/coinapi_pair_metadata.json'
        
        coinapi_eth_pairs_str = ('[' + self.s3_connection.read_key(key = key, bucket_name = 'project-poseidon-data').strip() + ']').replace('}', '},').replace('},]', '}]')

        coinapi_eth_pairs_json = json.loads(coinapi_eth_pairs_str)
        coinapi_eth_pairs_df = pd.DataFrame(coinapi_eth_pairs_json)

        new_price_data = []

        for i in range(len(coinapi_eth_pairs_df)):
            eth_pair = coinapi_eth_pairs_df.iloc[i]

            print('{}) pair: {}/{} (exchange: {})'.format(i + 1, eth_pair['asset_id_base'], eth_pair['asset_id_quote'], eth_pair['exchange_id']))
            print()
            
            coinapi_symbol_id = eth_pair['exchange_id'] + '_' + 'SPOT' + '_' + eth_pair['asset_id_base'] + '_' + eth_pair['asset_id_quote']
            period_id = period_id_map[self.time_interval]
            time_start = self.__get_next_start_date(eth_pair)

            latest_price_data_for_pair = self.__get_latest_price_data(coinapi_symbol_id = coinapi_symbol_id,
                                                                      period_id = period_id, 
                                                                      time_start = time_start)

            if latest_price_data_for_pair == 429:
                print('Exceeded API key rate limits for today... task finished.')
                break
            elif latest_price_data_for_pair == None:
                print('No data returned for request... continuing to next pair.')
                continue
            else:
                print('got data for this pair... uploading to S3 and updating coinapi eth pairs metadata.')
                new_price_data.extend(latest_price_data_for_pair)

                self.__upload_new_price_data(new_price_data)
                self.__update_coinapi_eth_pairs_metadata(latest_price_data_for_pair,
                                                         coinapi_eth_pairs_df,
                                                         eth_pair['exchange_id'],
                                                         eth_pair['asset_id_base'],
                                                         eth_pair['asset_id_quote'])