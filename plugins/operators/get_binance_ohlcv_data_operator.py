import io
import zipfile
from airflow.models import BaseOperator
from datetime import timedelta

import pandas as pd
import duckdb
import time
import os
import requests as r

class GetBinanceOHLCVDataOperator(BaseOperator):
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_next_start_date(self, coinapi_token):
            """
            Calculates the next set of start dates for scraping price data for a given token.

            This method generates a list of the next ten valid start dates, considering the most recent scrape date. 
            It ensures that the dates are rounded to the nearest hour and are within the current UTC time.

            Parameters:
                coinapi_token (pandas.Series): A pandas Series containing metadata for a specific token.

            Returns:
                list of str: A list of ISO 8601 formatted start dates.
            """
                
            # Get the next start date for the current token
            next_start_date = coinapi_token['candle_data_end']

            # If there is no next start date, use the token's initial start date
            if pd.isnull(next_start_date):
                return pd.to_datetime(coinapi_token['candle_data_start'], unit = 'ms')
            else:
                return pd.to_datetime(next_start_date, unit = 'ms')   
 
    def _upload_new_ohlcv_data(self, ohlcv_data, year, month):
        """
        Uploads newly collected trade data to DuckDB.

        This method converts the trade data to JSON format and uploads it to a specified S3 bucket. 
        It handles the process of chunking data and managing file names for storage.

        Returns:
            None
        """
        # Create temporary file to store OHLCV data
        data_to_upload = pd.DataFrame(ohlcv_data)
        symbol_id = f"{data_to_upload['asset_id_base'].iloc[0]}_{data_to_upload['asset_id_quote'].iloc[0]}_{data_to_upload['exchange_id'].iloc[0]}"
        path = f'/Users/louisspencer/LocalData/data/ohlcv_data/{symbol_id}_{year}_{month}.csv.gz'
        data_to_upload.to_csv(path, index = False, compression = 'gzip')
        # Connect to DuckDB
        # with duckdb.connect(
        #     database = '/Users/louisspencer/LocalData/database.db',
        #     read_only = False
        # ) as conn:
        #     # Load the new order book data into the database
        #     query = f"""
        #     INSERT OR REPLACE INTO market_data.ohlcv_1m
        #     SELECT
        #         time_period_start,
        #         time_period_end,
        #         open,
        #         high,
        #         low,
        #         close,
        #         volume,
        #         trades,
        #         asset_id_base,
        #         asset_id_quote,
        #         exchange_id
        #     FROM read_csv('{path}')
        #     """
        #     conn.sql(query)
        #     conn.commit()
        #     conn.close()

        # Remove the temporary file
        # os.remove(path)

    def _update_coinapi_metadata(self, next_start_date, coinapi_token, coinapi_pairs_df):
            """
            Updates the next scrape date for a token in the local metadata.

            This method modifies the metadata DataFrame to reflect the next scrape date for a particular token, 
            based on the most recent successful data retrieval.

            Parameters:
                time_start (str): The start time of the most recent successful data retrieval.
                
                coinapi_token (pandas.Series): A pandas Series containing metadata for a specific token.

            Returns:
                None
            """

            asset_id_base = coinapi_token['asset_id_base']
            asset_id_quote = coinapi_token['asset_id_quote']
            exchange_id = coinapi_token['exchange_id']

            # Update next scrape date for current token locally
            predicate = (coinapi_pairs_df['exchange_id'] == exchange_id) & (coinapi_pairs_df['asset_id_base'] == asset_id_base) & (coinapi_pairs_df['asset_id_quote'] == asset_id_quote)
            coinapi_pairs_df.loc[predicate, 'candle_data_end'] = next_start_date

            # Write the metadata to a local JSON file
            metadata_path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/binance_metadata.json'
            coinapi_pairs_df.to_json(metadata_path, orient = 'records', lines = True)

    def _get_ohlcv_data(self, base, quote, exchange, time_start, coinapi_token, binance_metadata):
        """
        Retrieves a single order book snapshot from CoinAPI for a given token and time.

        Sends a request to the CoinAPI to fetch the order book data for a specific cryptocurrency token at 
        a given time. Handles various HTTP response statuses to identify successful and failed requests.

        Parameters:
            coinapi_symbol_id (str): The identifier for the cryptocurrency symbol in CoinAPI.

            time_start (str): The start time for the data request in ISO 8601 format.

        Returns:
            list or int: A list of a single order book snapshot if successful, otherwise an error code.
        """
        time_start = pd.to_datetime(time_start, unit = 'ms')
        time_year = time_start.year
        time_month = time_start.month

        if pd.isnull(time_year):
            time_year = 2019

        for year in range(int(time_year), 2026):
            for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
                if year < time_year or (year == time_year and int(month) <= time_month):
                    continue

                try:
                    url = f'https://data.binance.vision/data/spot/monthly/klines/{base}{quote}/1m/{base}{quote}-1m-{year}-{month}.zip'
                    print(f'Retrieving data for {month}-{year} from {url}....')
                    print()
                    response = r.get(url)

                    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                        with z.open(f'{base}{quote}-1m-{year}-{month}.csv') as f:
                            df = pd.read_csv(f, header = None)
                            df.columns = ['time_period_start', 'open', 'high', 'low', 'close', 'volume', 'time_period_end', 'quote_asset_volume', 'trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
                            df = df[['time_period_start', 'time_period_end', 'open', 'high', 'low', 'close', 'volume', 'trades']]

                            try:
                                df['time_period_start'] = pd.to_datetime(df['time_period_start'], unit = 'ms')
                                df['time_period_start'] = df['time_period_start'].dt.round('T')
                                df['time_period_end'] = pd.to_datetime(df['time_period_end'], unit = 'ms')
                                df['time_period_end'] = df['time_period_end'].dt.round('T')
                            except Exception as e:
                                df['time_period_start'] = pd.to_datetime(df['time_period_start'] * 1000, unit = 'ns')
                                df['time_period_start'] = df['time_period_start'].dt.round('T')
                                df['time_period_end'] = pd.to_datetime(df['time_period_end'] * 1000, unit = 'ns')
                                df['time_period_end'] = df['time_period_end'].dt.round('T')

                            df['asset_id_base'] = base
                            df['asset_id_quote'] = quote
                            df['exchange_id'] = exchange

                    if df.empty:
                        print(f'No data found for {base}{quote} in {month}-{year}. Skipping...')
                        continue

                    print(df.head())
                    print()

                    max_date = df['time_period_end'].max()

                    self._upload_new_ohlcv_data(df, year = year, month = month)
                    self._update_coinapi_metadata(next_start_date = max_date, coinapi_token = coinapi_token, coinapi_pairs_df = binance_metadata)

                except Exception as e:
                    print(type(e))
                    print(f'Error retrieving data from {url}....')
                    print(e)
                    print()
                    continue
               
    def execute(self, context):
        """
        Main execution function for the GetTickDataOperator.

        This method orchestrates the entire process of collecting and storing CoinAPI order book data for specified tokens. 
        It involves deleting existing data from S3, iterating over each token to collect new data, handling data synchronization 
        with S3, and ensuring continuous data retrieval until all required data points are collected or no more valid dates are available.

        Parameters:
            context (dict): The Airflow context object containing runtime information about the task.

        Returns:
            None
        """

        # File path for token metadata (last scrape dates)
        path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/binance_metadata.json'

        # Read token metadata from file and load it into a DataFrame
        binance_metadata = pd.read_json(path, lines = True)

        # For each token in DESIRED_TOKENS
        for i in range(len(binance_metadata)):
            if i < 172:
                continue

            # Get token metadata for current token
            coinapi_token = binance_metadata.iloc[i]
            symbol_id = coinapi_token['asset_id_base'] + '_' + coinapi_token['asset_id_quote'] + '_' + coinapi_token['exchange_id']

            self.log.info('GetBinanceOHLCVDataOperator: {}) token: {}/{} (exchange: {})'.format(i + 1, coinapi_token['asset_id_base'], coinapi_token['asset_id_quote'], coinapi_token['exchange_id']))
            self.log.info('GetBinanceOHLCVDataOperator: ')

            # Get the next start date for the current token
            next_start_date = self._get_next_start_date(binance_metadata.iloc[i])

            self.log.info('GetBinanceOHLCVDataOperator: ******* Getting OHLCV data from {} to {}'.format(next_start_date, next_start_date + timedelta(days = 30)))
            self.log.info('GetBinanceOHLCVDataOperator: ')
        
            # Get OHLCV data for the current token
            self._get_ohlcv_data(
                base = coinapi_token['asset_id_base'],
                quote = coinapi_token['asset_id_quote'],
                exchange = coinapi_token['exchange_id'],
                time_start = next_start_date,
                coinapi_token = coinapi_token,
                binance_metadata = binance_metadata
            )

            # Sleep for 1 second to avoid rate limiting
            time.sleep(1)
