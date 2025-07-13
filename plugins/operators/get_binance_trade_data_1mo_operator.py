from airflow.models import BaseOperator
from datetime import timedelta

import pandas as pd
import duckdb
import time
import os
import numpy as np
import zipfile
import io
import requests as r

class GetBinanceTradeDataOperator(BaseOperator):
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_next_start_date(self, coinapi_token):
        # Get the next start date for the current token
        next_start_date = pd.to_datetime(coinapi_token['trade_data_end'], unit = 'ms') 

        # If there is no next start date, use the token's initial start date
        if pd.isnull(next_start_date):
            return pd.to_datetime(coinapi_token['trade_data_start'], unit = 'ms')
        else:
            return next_start_date    
 
    def _upload_new_trade_data(self, trade_data, year, month):
        # Create temporary file to store trade data
        data_to_upload = pd.DataFrame(trade_data)
        symbol_id = f"{data_to_upload['asset_id_base'][0]}_{data_to_upload['asset_id_quote'][0]}_{data_to_upload['exchange_id'][0]}"
        path = f'/Users/louisspencer/LocalData/data/trade_data/{symbol_id}_{year}_{month}.csv.gz'
        data_to_upload.to_csv(path, index = False, compression = 'gzip')

        # Connect to DuckDB
        # with duckdb.connect(
        #     database = '/Users/louisspencer/LocalData/database.db',
        #     read_only = False
        # ) as conn:
        #     # Load the new order book data into the database
        #     query = f"""
        #     INSERT OR REPLACE INTO market_data.trade_data
        #     SELECT
        #         trade_id,
        #         timestamp,
        #         price,
        #         quantity,
        #         quote_quantity,
        #         side,
        #         is_best_match,
        #         asset_id_base,
        #         asset_id_quote,
        #         exchange_id
        #     FROM read_parquet('{path}')
        #     """
        #     conn.sql(query)
        #     conn.commit()
        #     conn.close()
        #
        # # Remove the temporary file
        # os.remove(path)

    def _update_coinapi_metadata(self, next_start_date, coinapi_token, coinapi_pairs_df):
        asset_id_base = coinapi_token['asset_id_base']
        asset_id_quote = coinapi_token['asset_id_quote']
        exchange_id = coinapi_token['exchange_id']

        # Update next scrape date for current token locally
        predicate = (coinapi_pairs_df['exchange_id'] == exchange_id) & (coinapi_pairs_df['asset_id_base'] == asset_id_base) & (coinapi_pairs_df['asset_id_quote'] == asset_id_quote)
        coinapi_pairs_df.loc[predicate, 'trade_data_end'] = next_start_date

        # Write the metadata to a local JSON file
        metadata_path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/binance_metadata.json'
        coinapi_pairs_df.to_json(metadata_path, orient = 'records', lines = True)

    def _get_trade_data(self, base, quote, exchange, time_start, coinapi_token, binance_metadata):
        time_start = pd.to_datetime(time_start, unit = 'ms')
        time_year = time_start.year
        time_month = time_start.month

        if pd.isnull(time_year):
            time_year = 2019

        for year in range(int(time_year), 2025):
            for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
                if year < time_year or (year == time_year and int(month) <= time_month):
                    continue

                try:
                    url = f'https://data.binance.vision/data/spot/monthly/trades/{base}{quote}/{base}{quote}-trades-{year}-{month}.zip'
                    print(f'Retrieving data for {month}-{year} from {url}....')
                    print()
                    response = r.get(url)

                    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                        with z.open(f'{base}{quote}-trades-{year}-{month}.csv') as f:
                            df = pd.read_csv(f, header = None)
                            df.columns = ['trade_id', 'price', 'qty', 'quote_qty', 'time', 'is_buyer_maker', 'is_best_match']
                            df['side'] = np.where(df['is_buyer_maker'] == True, 'sell', 'buy')
                            try:
                                df['time'] = pd.to_datetime(df['time'], unit = 'ms')
                            except Exception as e:
                                df['time'] = pd.to_datetime(df['time'] * 1000, unit = 'ns')
                            df['asset_id_base'] = base
                            df['asset_id_quote'] = quote
                            df['exchange_id'] = exchange

                            df = df.drop(columns = ['is_buyer_maker'])
                            df = df.rename(columns = {'time': 'timestamp', 'qty': 'quantity', 'quote_qty': 'quote_quantity'})
                            df = df[['trade_id', 'timestamp', 'price', 'quantity', 'quote_quantity', 'side', 'is_best_match', 'asset_id_base', 'asset_id_quote', 'exchange_id']].drop_duplicates(subset = ['trade_id'])
                            
                            print(df.head())
                            print()

                            max_date = df['timestamp'].max()

                            self._upload_new_trade_data(df)
                            self._update_coinapi_metadata(next_start_date = max_date, coinapi_token = coinapi_token, coinapi_pairs_df = binance_metadata)

                except Exception as e:
                    print(f'Error retrieving data from {url}....')
                    print(e)
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

            self.log.info('GetBinanceTradeDataOperator: {}) token: {}/{} (exchange: {})'.format(i + 1, coinapi_token['asset_id_base'], coinapi_token['asset_id_quote'], coinapi_token['exchange_id']))
            self.log.info('GetBinanceTradeDataOperator: ')

            # Get the next start date for the current token
            next_start_date = self._get_next_start_date(binance_metadata.iloc[i])

            self.log.info('GetBinanceTradeDataOperator: ******* Getting trade data from {} to {}'.format(next_start_date, next_start_date + timedelta(days = 30)))
            self.log.info('GetBinanceTradeDataOperator: ')
        
            # Get trade data for the current token
            self._get_trade_data(
                base = coinapi_token['asset_id_base'],
                quote = coinapi_token['asset_id_quote'],
                exchange = coinapi_token['exchange_id'],
                time_start = next_start_date,
                coinapi_token = coinapi_token,
                binance_metadata = binance_metadata
            )

            # Sleep for 1 second to avoid hitting API rate limits
            time.sleep(1)
