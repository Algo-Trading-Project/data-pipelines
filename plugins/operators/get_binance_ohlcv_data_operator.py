from airflow.models import BaseOperator
from datetime import timedelta

import pandas as pd
import asyncio
import aiohttp
import zipfile
import io
import pathlib
import pendulum

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
 
    def _upload_new_ohlcv_data(self, ohlcv_data, time_start):
        """
        Uploads newly collected trade data to DuckDB.

        This method converts the trade data to JSON format and uploads it to a specified S3 bucket. 
        It handles the process of chunking data and managing file names for storage.

        Returns:
            None
        """
        data_to_upload = pd.DataFrame(ohlcv_data)
        date = time_start.strftime('%Y-%m-%d')
        symbol_id = f"{data_to_upload['asset_id_base'].iloc[0]}_{data_to_upload['asset_id_quote'].iloc[0]}_{data_to_upload['exchange_id'].iloc[0]}"
        output_path = f'/Users/louisspencer/LocalData/data/ohlcv_data/symbol_id={symbol_id}/date={date}/ohlcv_data.parquet'
        data_to_upload.to_parquet(output_path, index = False, compression = 'snappy')

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

    async def _get_ohlcv_data(self, session, sem, time_start, coinapi_token, binance_metadata):
        year = time_start.year
        month = f'{time_start.month:02d}'
        day = f'{time_start.day:02d}'
        base, quote, exchange = coinapi_token['asset_id_base'], coinapi_token['asset_id_quote'], coinapi_token['exchange_id']
        url = f'https://data.binance.vision/data/spot/daily/klines/{base}{quote}/1m/{base}{quote}-1m-{year}-{month}-{day}.zip'
        print(f'Retrieving data for {month}-{day}-{year} for {base}/{quote}...')
        print()
        try:
            async with sem:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status != 200:
                        print(f'Error retrieving data for {month}-{year}-{day}: {response.status}')
                        print(f'URL: {url}')
                        print(f'Response: {await response.text()}')
                        print()
                        return
                    content = await response.read()
        except Exception as e:
            print(f'Error retrieving data for {month}-{year}-{day}: {str(e)}')
            print(f'URL: {url}')
            print()
            return

        with zipfile.ZipFile(io.BytesIO(content)) as z:
            with z.open(f'{base}{quote}-1m-{year}-{month}-{day}.csv') as f:
                df = pd.read_csv(f, header = None)

        if df.empty:
            print(f'No data found for {base}/{quote} in {month}-{year}-{day}.')
            return
        
        df.columns = ['time_period_start', 'open', 'high', 'low', 'close', 'volume', 'time_period_end', 'quote_asset_volume', 'trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
        df = df[['time_period_start', 'time_period_end', 'open', 'high', 'low', 'close', 'volume', 'trades']]
        df['asset_id_base'] = base
        df['asset_id_quote'] = quote
        df['exchange_id'] = exchange

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

        print(df.head())
        print()
        self._upload_new_ohlcv_data(df, time_start)
        # Operator only gets yesterday's data, so no need to update the metadata
        # self._update_coinapi_metadata(next_start_date = max_date, coinapi_token = coinapi_token, coinapi_pairs_df = binance_metadata)
               
    def execute(self, context):
        path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/binance_metadata.json'
        binance_metadata = pd.read_json(path, lines = True)
        exec_date = context['execution_date'].astimezone(pendulum.timezone('UTC'))
        target_day = (exec_date - timedelta(days = 1)).date()
        self.log.info('GetBinanceOHLCVDataOperator: Executing for target date: {}'.format(target_date))

        async def _runner():
            sem = asyncio.Semaphore(30)
            async with aiohttp.ClientSession() as session:
                tasks = []
                for i in range(len(binance_metadata)):
                    tasks.append(
                        self._get_ohlcv_data(
                            session, 
                            sem, 
                            target_day,
                            binance_metadata.iloc[i],
                            binance_metadata
                        )
                    )
                        
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for e in [r for r in results if isinstance(r, Exception)]:
                    self.log.warning(f'Error in task: {e}')

        asyncio.run(_runner())


        # save updated metadata once at end
        binance_metadata.to_json(path, orient = 'records', lines = True)