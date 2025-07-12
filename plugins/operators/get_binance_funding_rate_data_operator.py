from airflow.models import BaseOperator
from airflow.models import Variable
from datetime import timedelta, datetime

import json
import pandas as pd
import dateutil.parser as parser
import duckdb
import time
import subprocess
import os
import random
import numpy as np
import zipfile
import io
import requests as r

class GetBinanceFundingRateOperator(BaseOperator): 
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_next_start_date(self, coinapi_token):
        # Get the next start date for the current token
        next_start_date = pd.to_datetime(coinapi_token['funding_rate_data_end'], unit = 'ms') 
        print(f'Next start date for {coinapi_token["asset_id_base"]}_{coinapi_token["asset_id_quote"]}_{coinapi_token["exchange_id"]}: {next_start_date}')

        # If there is no next start date, use the token's initial start date
        if pd.isnull(next_start_date):
            return pd.to_datetime(coinapi_token['funding_rate_data_start'], unit = 'ms')
        else:
            return next_start_date    
 
    def _upload_new_funding_rate_data(self, funding_rate_data, year, month):
        print('Uploading new futures trade data to DuckDB....')
        print()
        symbol_id = funding_rate_data['asset_id_base'].iloc[0] + '_' + funding_rate_data['asset_id_quote'].iloc[0] + '_' + funding_rate_data['exchange_id'].iloc[0]
        
        # Create temporary file to store trade data
        path = f'/Users/louisspencer/LocalData/data/funding_rate_data/{symbol_id}-{year}-{month}.parquet'
        data_to_upload = pd.DataFrame(funding_rate_data)
        data_to_upload.to_parquet(path, index = False, compression = 'snappy')

        # Connect to DuckDB
        # with duckdb.connect(
        #     database = '/Users/louisspencer/LocalData/database.db',
        #     read_only = False
        # ) as conn:
        #     # Load the new order book data into the database
        #     query = f"""
        #     INSERT INTO market_data.stg_futures_trade_data
        #     SELECT
        #         trade_id,
        #         timestamp,
        #         price,
        #         quantity,
        #         quote_quantity,
        #         side,
        #         asset_id_base,
        #         asset_id_quote,
        #         exchange_id
        #     FROM read_parquet('{path}')
        #     """
        #     conn.sql(query)
        #     conn.commit()
        #     conn.close()

        # # Remove the temporary file
        # os.remove(path)

    def _update_coinapi_metadata(self, next_start_date, coinapi_token, coinapi_pairs_df):
        asset_id_base = coinapi_token['asset_id_base']
        asset_id_quote = coinapi_token['asset_id_quote']
        exchange_id = coinapi_token['exchange_id']

        # Update next scrape date for current token locally
        predicate = (coinapi_pairs_df['exchange_id'] == exchange_id) & (coinapi_pairs_df['asset_id_base'] == asset_id_base) & (coinapi_pairs_df['asset_id_quote'] == asset_id_quote)
        coinapi_pairs_df.loc[predicate, 'funding_rate_data_end'] = next_start_date

        # Write the metadata to a local JSON file
        metadata_path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/binance_metadata.json'
        coinapi_pairs_df.to_json(metadata_path, orient = 'records', lines = True)

    def _get_funding_rate_data(self, base, quote, exchange, time_start, coinapi_token, binance_metadata):
        time_start = pd.to_datetime(time_start, unit = 'ms')
        time_year = time_start.year
        time_month = time_start.month
        
        if pd.isnull(time_year):
            time_year = 2019

        for year in range(int(time_year), 2026):
            for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
                if year < time_year or (year == time_year and int(month) < time_month):
                    continue
                curr_date = datetime(year, int(month), 1)
                if curr_date > datetime.utcnow():
                    continue
                try:
                    url = f'https://data.binance.vision/data/futures/um/monthly/fundingRate/{base}{quote}/{base}{quote}-fundingRate-{year}-{month}.zip'
                    print(f'Retrieving data for {month}-{year} from {url}....')
                    print()
                    response = r.get(url)

                    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                        with z.open(f'{base}{quote}-fundingRate-{year}-{month}.csv') as f:
                            df = pd.read_csv(f, header = 0)
                            df.columns = ['timestamp', 'funding_interval_hours', 'last_funding_rate']
                            df['timestamp'] = pd.to_datetime(df['timestamp'], unit = 'ms')
                            df['asset_id_base'] = base
                            df['asset_id_quote'] = quote
                            df['exchange_id'] = exchange

                    print(df.head())
                    print()
                    max_date = df['timestamp'].max()

                    self._upload_new_funding_rate_data(
                        funding_rate_data = df,
                        year = year,
                        month = month
                    )
                    self._update_coinapi_metadata(
                        next_start_date = max_date, 
                        coinapi_token = coinapi_token, 
                        coinapi_pairs_df = binance_metadata
                    )

                except Exception as e:
                    print(f'Error retrieving data from {url}....')
                    print(e)
                    print()
                    continue
               
    def execute(self, context):
        # File path for token metadata (last scrape dates)
        path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/binance_metadata.json'

        # Read token metadata from file and load it into a DataFrame
        binance_metadata = pd.read_json(path, lines = True)

        # For each token in DESIRED_TOKENS
        for i in range(len(binance_metadata)):
            # Get token metadata for current token
            coinapi_token = binance_metadata.iloc[i]
            symbol_id = coinapi_token['asset_id_base'] + '_' + coinapi_token['asset_id_quote'] + '_' + coinapi_token['exchange_id']
            if not coinapi_token['asset_id_quote'] in ['USDT', 'USDC', 'BUSD']:
                continue

            self.log.info('GetBinanceFundingRateOperator: {}) token: {}/{} (exchange: {})'.format(i + 1, coinapi_token['asset_id_base'], coinapi_token['asset_id_quote'], coinapi_token['exchange_id']))
            self.log.info('GetBinanceFundingRateOperator: ')

            # Get the next start date for the current token
            next_start_date = self._get_next_start_date(binance_metadata.iloc[i])

            self.log.info('GetBinanceFundingRateOperator: ******* Getting funding rate data from {} to {}'.format(next_start_date, next_start_date + timedelta(days = 30)))
            self.log.info('GetBinanceFundingRateOperator: ')
        
            # Get futures data for the current token
            self._get_funding_rate_data(
                base = coinapi_token['asset_id_base'],
                quote = coinapi_token['asset_id_quote'],
                exchange = coinapi_token['exchange_id'],
                time_start = next_start_date,
                coinapi_token = coinapi_token,
                binance_metadata = binance_metadata
            )

            # Sleep for 3 second to avoid hitting API rate limits
            # time.sleep(3)
