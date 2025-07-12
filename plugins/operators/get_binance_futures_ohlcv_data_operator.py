from airflow.models import BaseOperator
from airflow.models import Variable
from datetime import timedelta, datetime

import pandas as pd
import zipfile
import io
import pendulum
import aiohttp
import asyncio
import pathlib

class GetBinanceFuturesOHLCVDataOperator(BaseOperator):
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_next_start_date(self, coinapi_token):                
        # Get the next start date for the current token
        next_start_date = pd.to_datetime(coinapi_token['futures_candle_data_end'], unit = 'ms') 

        # If there is no next start date, use the token's initial start date
        if pd.isnull(next_start_date):
            return pd.to_datetime(coinapi_token['futures_candle_data_start'], unit = 'ms')
        else:
            return next_start_date    
 
    def _upload_new_futures_ohlcv_data(self, futures_ohlcv_data, time_start):
        data_to_upload = pd.DataFrame(futures_ohlcv_data)
        date = time_start.strftime('%Y-%m-%d')
        symbol_id = f'{data_to_upload["asset_id_base"].iloc[0]}_{data_to_upload["asset_id_quote"].iloc[0]}_{data_to_upload["exchange_id"].iloc[0]}'
        output_path = f'~/LocalData/data/futures_ohlcv_data/symbol_id={symbol_id}/date={date}/futures_ohlcv_data.parquet'
        data_to_upload.to_parquet(output_path, index=False, compression='snappy')
            
    def _update_coinapi_metadata(self, next_start_date, coinapi_token, coinapi_pairs_df):
            asset_id_base = coinapi_token['asset_id_base']
            asset_id_quote = coinapi_token['asset_id_quote']
            exchange_id = coinapi_token['exchange_id']

            # Update next scrape date for current token locally
            predicate = (coinapi_pairs_df['exchange_id'] == exchange_id) & (coinapi_pairs_df['asset_id_base'] == asset_id_base) & (coinapi_pairs_df['asset_id_quote'] == asset_id_quote)
            coinapi_pairs_df.loc[predicate, 'futures_candle_data_end'] = next_start_date

            # Write the metadata to a local JSON file
            metadata_path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/binance_metadata.json'
            coinapi_pairs_df.to_json(metadata_path, orient = 'records', lines = True)

    async def _get_futures_ohlcv_data(self, session, sem, time_start, coinapi_token, binance_metadata):
        year = time_start.year
        month = f'{time_start.month:02d}'
        day = f'{time_start.day:02d}'
        base, quote, exchange = coinapi_token['asset_id_base'], coinapi_token['asset_id_quote'], coinapi_token['exchange_id']
        url = f'https://data.binance.vision/data/futures/um/daily/klines/{base}{quote}/1m/{base}{quote}-1m-{year}-{month}-{day}.zip'
        print(f'Retrieving data on {month}-{day}-{year} for {base}/{quote}...')
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
            print(f'No data found for {base}/{quote} on {month}-{day}-{year}.')
            return

        df.columns = ['time_period_start', 'open', 'high', 'low', 'close', 'volume', 'time_period_end', 'quote_volume', 'trades', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore']
        df = df[['time_period_start', 'time_period_end', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades', 'taker_buy_volume', 'taker_buy_quote_volume']]
        df['asset_id_base'] = base
        df['asset_id_quote'] = quote
        df['exchange_id'] = exchange

        try:
            df['time_period_start'] = pd.to_datetime(df['time_period_start'], unit='ms').dt.round('1min')
            df['time_period_end'] = pd.to_datetime(df['time_period_end'], unit='ms').dt.round('1min')
            max_date = df['time_period_end'].max()
        except Exception as e:
            # drop first row since it has header info
            df = df.iloc[1:]
            df['time_period_start'] = pd.to_datetime(df['time_period_start'], unit='ms').dt.round('1min')
            df['time_period_end'] = pd.to_datetime(df['time_period_end'], unit='ms').dt.round('1min')
            max_date = df['time_period_end'].max()

        print(df.head())
        print()
        self._upload_new_futures_ohlcv_data(df, time_start=time_start)
        # Operator only gets yesterday's data, so no need to update the metadata
        # self._update_coinapi_metadata(next_start_date = max_date, coinapi_token = coinapi_token, coinapi_pairs_df = binance_metadata)
               
    def execute(self, context):
        # File path for token metadata (last scrape dates)
        path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/binance_metadata.json'

        # Read token metadata from file and load it into a DataFrame
        binance_metadata = pd.read_json(path, lines = True)

        # target date = previous UTC day of execution_date
        exec_date = context['execution_date'].astimezone(pendulum.timezone('UTC'))
        target_day = (exec_date - timedelta(days=1)).date()
        self.log.info("Fetching daily klines for %s", target_day)

        # Function to parallelize the download of daily klines for each symbol
        async def _runner():
            sem = asyncio.Semaphore(30)  # 30 concurrent HTTP requests
            async with aiohttp.ClientSession() as session:
                tasks = []
                for i in range(len(binance_metadata)):
                    base = binance_metadata.iloc[i]['asset_id_base']
                    quote = binance_metadata.iloc[i]['asset_id_quote']
                    exchange = binance_metadata.iloc[i]['exchange_id']
                    coinapi_token = binance_metadata.iloc[i]
                    tasks.append(
                        self._get_futures_ohlcv_data(
                            session=session,
                            sem=sem,
                            time_start=target_day,
                            coinapi_token=coinapi_token,
                            binance_metadata=binance_metadata
                        )
                    )
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for e in [r for r in results if isinstance(r, Exception)]:
                    self.log.warning(f'Error in task: {e}')
                
        asyncio.run(_runner())

        # save updated metadata once at end
        binance_metadata.to_json(path, orient='records', lines=True)