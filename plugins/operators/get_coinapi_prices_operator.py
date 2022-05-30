from airflow.models import BaseOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from pendulum import date

import requests as r
import json
import pandas as pd
import datetime
import dateutil.parser as parser

class GetCoinAPIPricesOperator(BaseOperator):

    def __init__(self, time_interval, **kwargs):
        super().__init__(**kwargs)

        self.time_interval = time_interval        
        self.s3_connection = S3Hook(aws_conn_id = 's3_conn')

    def __get_next_start_date(self, eth_pair):
        most_recent_data_date = eth_pair['latest_scrape_date_1_{}'.format(self.time_interval)]
        next_start_date = None

        time_delta_map = {
            'min':datetime.timedelta(minutes = 1),
            'hour':datetime.timedelta(hours = 1),
            'day':datetime.timedelta(days = 1)
        }

        if pd.isnull(most_recent_data_date):
            next_start_date = parser.parse(str(eth_pair['data_start'])).isoformat()
        else:
            next_start_date_str = str(pd.to_datetime(most_recent_data_date) + time_delta_map[self.time_interval])
            next_start_date = parser.parse(next_start_date_str).isoformat()

        return next_start_date

    def __upload_new_price_data(self, new_price_data):
        data_to_uplaod = json.dumps(new_price_data).replace('[', '').replace(']', '').replace('},', '}')
        key = 'eth_data/price_data/coinapi_eth_pair_prices_1_{}'.format(self.time_interval)

        self.s3_connection.load_string(
            string_data = data_to_uplaod, 
            key = key, 
            bucket_name = 'project-poseidon-data', 
            replace = True
        )

    def __update_coinapi_eth_pairs_metadata(self, latest_price_data_for_pair, coinapi_eth_pairs_df, asset):
        sort_key = lambda x: pd.to_datetime(x['time_period_start'])

        element_w_latest_date = max(latest_price_data_for_pair, key = sort_key)
        new_latest_scrape_date = parser.parse(element_w_latest_date['time_period_start']).isoformat()

        coinapi_eth_pairs_df.loc[coinapi_eth_pairs_df['asset_id_base'] == asset, 'latest_scrape_date_1_{}'.format(self.time_interval)] = new_latest_scrape_date
        coinapi_eth_pairs_json = coinapi_eth_pairs_df.to_dict(orient = 'records')
        coinapi_eth_pairs_str = json.dumps(coinapi_eth_pairs_json).replace('[', '').replace(']', '').replace('},', '}')

        self.s3_connection.load_string(
            string_data = coinapi_eth_pairs_str,
            key = 'eth_data/price_data/coinapi_eth_pairs.json',
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
        headers = {'X-CoinAPI-Key':'3D7A02BB-EC62-40F9-8C5C-8625AB51D5ED'}
        
        response = r.get(
            url = api_request_url,
            headers = headers,
        ).json()

        print('api request url: {}'.format(api_request_url))
        print(response)
        print()

        # Error occurred during request
        if type(response) == dict and response.get('error') != None:
            return None

        # Request returned no data
        elif type(response) == list and len(response) == 0:
            return None

        formatted_response = format_response_data(response)

        return formatted_response

    def execute(self, context):
        period_id_map = {'min':'1MIN', 'hour':'1HRS', 'day':'1DAY'}
        key = 'eth_data/price_data/coinapi_eth_pairs.json'
        
        coinapi_eth_pairs_str = self.s3_connection.read_key(key = key, bucket_name = 'project-poseidon-data')
        coinapi_eth_pairs_json = json.loads(coinapi_eth_pairs_str)
        coinapi_eth_pairs_df = pd.DataFrame(coinapi_eth_pairs_json)

        new_price_data = []

        for i in range(len(coinapi_eth_pairs_df)):
            eth_pair = coinapi_eth_pairs_df.iloc[i]

            print('{}) pair: {}/ETH'.format(i + 1, eth_pair['asset_id_base']))
            print()
            
            coinapi_symbol_id = eth_pair['exchange_id'] + '_' + 'SPOT' + '_' + eth_pair['asset_id_base'] + '_' + eth_pair['asset_id_quote']
            period_id = period_id_map[self.time_interval]
            time_start = self.__get_next_start_date(eth_pair)

            latest_price_data_for_pair = self.__get_latest_price_data(coinapi_symbol_id = coinapi_symbol_id,
                                                                      period_id = period_id, 
                                                                      time_start = time_start)

            if latest_price_data_for_pair == None:
                print('last request did not return data.... task finished.')
                break
            else:
                new_price_data.extend(latest_price_data_for_pair)

                self.__upload_new_price_data(new_price_data)
                self.__update_coinapi_eth_pairs_metadata(latest_price_data_for_pair,
                                                         coinapi_eth_pairs_df, 
                                                         eth_pair['asset_id_base'])
