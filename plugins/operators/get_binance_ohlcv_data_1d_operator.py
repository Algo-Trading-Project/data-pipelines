from airflow.models import BaseOperator
from datetime import timedelta
from airflow.models import Variable

import pandas as pd
import asyncio
import aiohttp
import zipfile
import io
import pathlib
import pendulum
import duckdb
import os

class GetBinanceOHLCVDataDailyOperator(BaseOperator):
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _upload_new_ohlcv_data(self, ohlcv_data, time_start):
        data_to_upload = pd.DataFrame(ohlcv_data)
        symbol_id = f"{data_to_upload['asset_id_base'].iloc[0]}_{data_to_upload['asset_id_quote'].iloc[0]}_{data_to_upload['exchange_id'].iloc[0]}"
        data_to_upload['symbol_id'] = symbol_id
        date = time_start.strftime('%Y-%m-%d')
        
        s3_dir = f's3://base-44-data/data/ohlcv_data/raw/symbol_id={symbol_id}/date={date}/ohlcv_data.parquet'

        # Retrieve AWS credentials from Airflow Variables (Astronomer)
        aws_key = Variable.get("AWS_ACCESS_KEY_ID")
        aws_secret = Variable.get("AWS_SECRET_ACCESS_KEY")
        aws_region = Variable.get("AWS_DEFAULT_REGION")

        # Set environment for DuckDB (legacy auth scheme uses env vars)
        os.environ['AWS_ACCESS_KEY_ID'] = aws_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret
        os.environ['AWS_DEFAULT_REGION'] = aws_region
        
        # Register the data_to_upload DataFrame as a DuckDB table
        con = duckdb.connect(database=':memory:')
        con.register('data_to_upload', data_to_upload)

        # Use DuckDB to write the DataFrame to Parquet on S3
        con.execute(
            f"""
            COPY data_to_upload
            TO {s3_dir} (
                FORMAT 'PARQUET',
                COMPRESSION 'SNAPPY',
                OVERWRITE
            );
            """
        )

    # TODO: Refactor to write logs to S3 instead of locally
    def _write_to_log(self, path, data):
        # Ensure the directory exists or create it
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'a') as f:
            f.write(data)

    async def _get_ohlcv_data(self, session, sem, time_start, coinapi_token, binance_metadata):
        year = time_start.year
        month = f'{time_start.month:02d}'
        day = f'{time_start.day:02d}'
        base, quote, exchange = coinapi_token['asset_id_base'], coinapi_token['asset_id_quote'], coinapi_token['exchange_id']
        url = f'https://data.binance.vision/data/spot/daily/klines/{base}{quote}/1m/{base}{quote}-1m-{year}-{month}-{day}.zip'
        self.log.info(f'Retrieving data for {month}-{day}-{year} for {base}/{quote}...')

        # Request data from Binance
        try:
            async with sem:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status != 200:
                        self.log.warning(f'Error retrieving data for {month}-{year}-{day}: {response.status} for {base}/{quote} from {url}')
                        # Log missing data to a file for later review
                        # log_path = Path.home() / "LocalData" / "data" / "ohlcv_data" / "logs" / "error_log.log"
                        # symbol_id = f"{base}_{quote}_{exchange}"
                        # # date, symbol_id, HTTP status code (optional), Exception message (optional)
                        # row = f'{year}-{month}-{day},{symbol_id},{response.status},null' + '\n'
                        # self._write_to_log(log_path, row)
                        return
                        
                    content = await response.read()

            with zipfile.ZipFile(io.BytesIO(content)) as z:
                with z.open(f'{base}{quote}-1m-{year}-{month}-{day}.csv') as f:
                    df = pd.read_csv(f, header = None)

        except Exception as e:
            self.log.warning(f'Error retrieving data for {month}-{year}-{day}: {str(e)} for {base}/{quote} from {url}')
            self.log.exception(e)
            
            # Log missing data to a file for later review
            # log_dir = Path.home() / "LocalData" / "data" / "ohlcv_data" / "logs" / "error_log.log"
            # symbol_id = f"{base}_{quote}_{exchange}"
            # # date, symbol_id, HTTP status code (optional), Exception message (optional)
            # row = f'{year}-{month}-{day},{symbol_id},null,{str(e)}' + '\n'
            # self._write_to_log(log_dir, row)
            return

        # Check if DataFrame is empty
        if df.empty:
            self.log.info(f'No data found for {base}/{quote} on {month}-{year}-{day}.')
            # Log missing data to a file for later review
            # log_dir = Path.home() / "LocalData" / "data" / "ohlcv_data" / "logs" / "error_log.log"
            # symbol_id = f"{base}_{quote}_{exchange}"
            # # date, symbol_id, HTTP status code (optional), Exception message (optional)
            # row = f'{year}-{month}-{day},{symbol_id},null,Empty' + '\n'
            # self._write_to_log(log_dir, row)
            return
        
        df.columns = ['time_period_start', 'open', 'high', 'low', 'close', 'volume', 'time_period_end', 'quote_asset_volume', 'trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
        df = df[['time_period_start', 'time_period_end', 'open', 'high', 'low', 'close', 'volume', 'trades']]
        df['asset_id_base'] = base
        df['asset_id_quote'] = quote
        df['exchange_id'] = exchange

        try:
            df['time_period_start'] = pd.to_datetime(df['time_period_start'], unit = 'ms')
            df['time_period_start'] = df['time_period_start'].dt.round('min')
            df['time_period_end'] = pd.to_datetime(df['time_period_end'], unit = 'ms')
            df['time_period_end'] = df['time_period_end'].dt.round('min')
        except Exception as e:
            df['time_period_start'] = pd.to_datetime(df['time_period_start'] * 1000, unit = 'ns')
            df['time_period_start'] = df['time_period_start'].dt.round('min')
            df['time_period_end'] = pd.to_datetime(df['time_period_end'] * 1000, unit = 'ns')
            df['time_period_end'] = df['time_period_end'].dt.round('min')

        # Fill potentially missing minutes and forward fill
        df = df.set_index('time_period_end').asfreq('1min').ffill().reset_index()
        self._upload_new_ohlcv_data(df, time_start)

        # Operator only gets yesterday's data, so no need to update the metadata
        # self._update_coinapi_metadata(next_start_date = max_date, coinapi_token = coinapi_token, coinapi_pairs_df = binance_metadata)
               
    def execute(self, context):
        # Load Binance metadata locally in Astronomer
        path = '/usr/local/airflow/include/binance_metadata.json'
        binance_metadata = pd.read_json(path, lines = True)
        exec_date = context['logical_date'].astimezone(pendulum.timezone('UTC'))
        target_date = (exec_date - pd.Timedelta(days = 1)).date()
        
        self.log.info('Fetching 1 day of MINUTE OHLCV data for Binance Spot for all symbols...')

        # Function to parallelize the download of daily OHLCV data for all tokens
        async def _runner():
            sem = asyncio.Semaphore(10)
            async with aiohttp.ClientSession() as session:
                tasks = []
                for i in range(len(binance_metadata)):
                    if binance_metadata.iloc[i]['asset_id_quote'] != 'USDT':
                        continue
                    tasks.append(
                        self._get_ohlcv_data(
                            session, 
                            sem, 
                            target_date,
                            binance_metadata.iloc[i],
                            binance_metadata
                        )
                    )
                        
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for e in [r for r in results if isinstance(r, Exception)]:
                    self.log.warning(f'Error in task: {e}')

        asyncio.run(_runner())