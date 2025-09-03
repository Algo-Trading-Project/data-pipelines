import duckdb
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ----------------------- DuckDB SQL (parameterised) -----------------------
_SQL_TEMPLATE = """
WITH base AS (
    SELECT
        *,
        asset_id_base || '_' || asset_id_quote || '_' || exchange_id AS symbol_id
    FROM read_parquet(glob('{input_root}/symbol_id=*/date=*/*.parquet'), hive_partitioning=true)
    WHERE date >= DATE '{cutoff}'
),
WITH rolling AS (
    SELECT
        date,
        asset_id_base,
        asset_id_quote,
        exchange_id,
        symbol_id,

        -- Total dollar volume (1-365d)
        total_dollar_volume AS total_dollar_volume_1d,
        SUM(total_dollar_volume) OVER w7 AS total_dollar_volume_7d,
        SUM(total_dollar_volume) OVER w30 AS total_dollar_volume_30d,
        SUM(total_dollar_volume) OVER w90 AS total_dollar_volume_90d,
        SUM(total_dollar_volume) OVER w180 AS total_dollar_volume_180d,
        SUM(total_dollar_volume) OVER w365 AS total_dollar_volume_365d,

        -- Total buy dollar volume (1-365d)
        total_buy_dollar_volume AS total_buy_dollar_volume_1d,
        SUM(total_buy_dollar_volume) OVER w7 AS total_buy_dollar_volume_7d,
        SUM(total_buy_dollar_volume) OVER w30 AS total_buy_dollar_volume_30d,
        SUM(total_buy_dollar_volume) OVER w90 AS total_buy_dollar_volume_90d,
        SUM(total_buy_dollar_volume) OVER w180 AS total_buy_dollar_volume_180d,
        SUM(total_buy_dollar_volume) OVER w365 AS total_buy_dollar_volume_365d,
        
        -- Total sell dollar volume (1-365d)
        total_sell_dollar_volume AS total_sell_dollar_volume_1d,
        SUM(total_sell_dollar_volume) OVER w7 AS total_sell_dollar_volume_7d,
        SUM(total_sell_dollar_volume) OVER w30 AS total_sell_dollar_volume_30d,
        SUM(total_sell_dollar_volume) OVER w90 AS total_sell_dollar_volume_90d,
        SUM(total_sell_dollar_volume) OVER w180 AS total_sell_dollar_volume_180d,
        SUM(total_sell_dollar_volume) OVER w365 AS total_sell_dollar_volume_365d,

        -- Number of buys (1-365d)
        num_buys AS num_buys_1d,
        SUM(num_buys) OVER w7 AS num_buys_7d,
        SUM(num_buys) OVER w30 AS num_buys_30d,
        SUM(num_buys) OVER w90 AS num_buys_90d,
        SUM(num_buys) OVER w180 AS num_buys_180d,
        SUM(num_buys) OVER w365 AS num_buys_365d,

        -- Number of sells (1-365d)
        num_sells AS num_sells_1d,
        SUM(num_sells) OVER w7 AS num_sells_7d,
        SUM(num_sells) OVER w30 AS num_sells_30d,
        SUM(num_sells) OVER w90 AS num_sells_90d,
        SUM(num_sells) OVER w180 AS num_sells_180d,
        SUM(num_sells) OVER w365 AS num_sells_365d,

        -- Average volume (1-365d)
        avg_dollar_volume AS avg_dollar_volume_1d,
        SUM(total_dollar_volume) OVER w7 / NULLIF(SUM(num_buys + num_sells) OVER w7, 0) AS avg_dollar_volume_7d,
        SUM(total_dollar_volume) OVER w30 / NULLIF(SUM(num_buys + num_sells) OVER w30, 0) AS avg_dollar_volume_30d,
        SUM(total_dollar_volume) OVER w90 / NULLIF(SUM(num_buys + num_sells) OVER w90, 0) AS avg_dollar_volume_90d,
        SUM(total_dollar_volume) OVER w180 / NULLIF(SUM(num_buys + num_sells) OVER w180, 0) AS avg_dollar_volume_180d,
        SUM(total_dollar_volume) OVER w365 / NULLIF(SUM(num_buys + num_sells) OVER w365, 0) AS avg_dollar_volume_365d,

        -- Average buy dollar volume (1-365d)
        avg_buy_dollar_volume AS avg_buy_dollar_volume_1d,
        SUM(total_buy_dollar_volume) OVER w7 / NULLIF(SUM(num_buys) OVER w7, 0) AS avg_buy_dollar_volume_7d,
        SUM(total_buy_dollar_volume) OVER w30 / NULLIF(SUM(num_buys) OVER w30, 0) AS avg_buy_dollar_volume_30d,
        SUM(total_buy_dollar_volume) OVER w90 / NULLIF(SUM(num_buys) OVER w90, 0) AS avg_buy_dollar_volume_90d,
        SUM(total_buy_dollar_volume) OVER w180 / NULLIF(SUM(num_buys) OVER w180, 0) AS avg_buy_dollar_volume_180d,
        SUM(total_buy_dollar_volume) OVER w365 / NULLIF(SUM(num_buys) OVER w365, 0) AS avg_buy_dollar_volume_365d,

        -- Average sell dollar volume (1-365d)
        avg_sell_dollar_volume AS avg_sell_dollar_volume_1d,
        SUM(total_sell_dollar_volume) OVER w7 / NULLIF(SUM(num_sells) OVER w7, 0) AS avg_sell_dollar_volume_7d,
        SUM(total_sell_dollar_volume) OVER w30 / NULLIF(SUM(num_sells) OVER w30, 0) AS avg_sell_dollar_volume_30d,
        SUM(total_sell_dollar_volume) OVER w90 / NULLIF(SUM(num_sells) OVER w90, 0) AS avg_sell_dollar_volume_90d,
        SUM(total_sell_dollar_volume) OVER w180 / NULLIF(SUM(num_sells) OVER w180, 0) AS avg_sell_dollar_volume_180d,
        SUM(total_sell_dollar_volume) OVER w365 / NULLIF(SUM(num_sells) OVER w365, 0) AS avg_sell_dollar_volume_365d,

        -- Dollar volume moments and percentiles (1d)
        std_dollar_volume AS std_dollar_volume_1d,
        skew_dollar_volume AS skew_dollar_volume_1d,
        kurtosis_dollar_volume AS kurtosis_dollar_volume_1d,
        "10th_percentile_dollar_volume" AS "10th_percentile_dollar_volume_1d",
        median_dollar_volume AS median_dollar_volume_1d,
        "90th_percentile_dollar_volume" AS "90th_percentile_dollar_volume_1d",

        -- Buy dollar volume 1d moments and percentiles
        std_buy_dollar_volume AS std_buy_dollar_volume_1d,
        skew_buy_dollar_volume AS skew_buy_dollar_volume_1d,
        kurtosis_buy_dollar_volume AS kurtosis_buy_dollar_volume_1d,
        "10th_percentile_buy_dollar_volume" AS "10th_percentile_buy_dollar_volume_1d",
        median_buy_dollar_volume AS median_buy_dollar_volume_1d,
        "90th_percentile_buy_dollar_volume" AS "90th_percentile_buy_dollar_volume_1d",

        -- Sell dollar volume 1d moments and percentiles
        std_sell_dollar_volume AS std_sell_dollar_volume_1d,
        skew_sell_dollar_volume AS skew_sell_dollar_volume_1d,
        kurtosis_sell_dollar_volume AS kurtosis_sell_dollar_volume_1d,
        "10th_percentile_sell_dollar_volume" AS "10th_percentile_sell_dollar_volume_1d",
        median_sell_dollar_volume AS median_sell_dollar_volume_1d,
        "90th_percentile_sell_dollar_volume" AS "90th_percentile_sell_dollar_volume_1d"
    FROM base
    WINDOW
        w7   AS (PARTITION BY symbol_id ORDER BY date ROWS BETWEEN 6   PRECEDING AND CURRENT ROW),
        w30  AS (PARTITION BY symbol_id ORDER BY date ROWS BETWEEN 29  PRECEDING AND CURRENT ROW),
        w90  AS (PARTITION BY symbol_id ORDER BY date ROWS BETWEEN 89  PRECEDING AND CURRENT ROW),
        w180 AS (PARTITION BY symbol_id ORDER BY date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW),
        w365 AS (PARTITION BY symbol_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)
),
feat AS (
    SELECT
        -- All columns from the rolling CTE
        *,

        -- Derived Columns from rolling CTE

        -- Num trades (1-365d)
        num_buys_1d + num_sells_1d AS num_trades_1d,
        num_buys_7d + num_sells_7d AS num_trades_7d,
        num_buys_30d + num_sells_30d AS num_trades_30d,
        num_buys_90d + num_sells_90d AS num_trades_90d,
        num_buys_180d + num_sells_180d AS num_trades_180d,
        num_buys_365d + num_sells_365d AS num_trades_365d,

        -- Pct buys (1-365d)
        1.0 * num_buys_1d / NULLIF(num_buys_1d + num_sells_1d, 0) AS pct_buys_1d,
        1.0 * num_buys_7d / NULLIF(num_buys_7d + num_sells_7d, 0) AS pct_buys_7d,
        1.0 * num_buys_30d / NULLIF(num_buys_30d + num_sells_30d, 0) AS pct_buys_30d,
        1.0 * num_buys_90d / NULLIF(num_buys_90d + num_sells_90d, 0) AS pct_buys_90d,
        1.0 * num_buys_180d / NULLIF(num_buys_180d + num_sells_180d, 0) AS pct_buys_180d,
        1.0 * num_buys_365d / NULLIF(num_buys_365d + num_sells_365d, 0) AS pct_buys_365d,

        -- Pct sells (1-365d)
        1.0 * num_sells_1d / NULLIF(num_buys_1d + num_sells_1d, 0) AS pct_sells_1d,
        1.0 * num_sells_7d / NULLIF(num_buys_7d + num_sells_7d, 0) AS pct_sells_7d,
        1.0 * num_sells_30d / NULLIF(num_buys_30d + num_sells_30d, 0) AS pct_sells_30d,
        1.0 * num_sells_90d / NULLIF(num_buys_90d + num_sells_90d, 0) AS pct_sells_90d,
        1.0 * num_sells_180d / NULLIF(num_buys_180d + num_sells_180d, 0) AS pct_sells_180d,
        1.0 * num_sells_365d / NULLIF(num_buys_365d + num_sells_365d, 0) AS pct_sells_365d,

        -- Pct buy dollar volume (1-365d)
        1.0 * total_buy_dollar_volume_1d / NULLIF(total_buy_dollar_volume_1d + total_sell_dollar_volume_1d, 0) AS pct_buy_dollar_volume_1d,
        1.0 * total_buy_dollar_volume_7d / NULLIF(total_buy_dollar_volume_7d + total_sell_dollar_volume_7d, 0) AS pct_buy_dollar_volume_7d,
        1.0 * total_buy_dollar_volume_30d / NULLIF(total_buy_dollar_volume_30d + total_sell_dollar_volume_30d, 0) AS pct_buy_dollar_volume_30d,
        1.0 * total_buy_dollar_volume_90d / NULLIF(total_buy_dollar_volume_90d + total_sell_dollar_volume_90d, 0) AS pct_buy_dollar_volume_90d,
        1.0 * total_buy_dollar_volume_180d / NULLIF(total_buy_dollar_volume_180d + total_sell_dollar_volume_180d, 0) AS pct_buy_dollar_volume_180d,
        1.0 * total_buy_dollar_volume_365d / NULLIF(total_buy_dollar_volume_365d + total_sell_dollar_volume_365d, 0) AS pct_buy_dollar_volume_365d,

        -- Pct sell dollar volume (1-365d)
        1.0 * total_sell_dollar_volume_1d / NULLIF(total_buy_dollar_volume_1d + total_sell_dollar_volume_1d, 0) AS pct_sell_dollar_volume_1d,
        1.0 * total_sell_dollar_volume_7d / NULLIF(total_buy_dollar_volume_7d + total_sell_dollar_volume_7d, 0) AS pct_sell_dollar_volume_7d,
        1.0 * total_sell_dollar_volume_30d / NULLIF(total_buy_dollar_volume_30d + total_sell_dollar_volume_30d, 0) AS pct_sell_dollar_volume_30d,
        1.0 * total_sell_dollar_volume_90d / NULLIF(total_buy_dollar_volume_90d + total_sell_dollar_volume_90d, 0) AS pct_sell_dollar_volume_90d,
        1.0 * total_sell_dollar_volume_180d / NULLIF(total_buy_dollar_volume_180d + total_sell_dollar_volume_180d, 0) AS pct_sell_dollar_volume_180d,
        1.0 * total_sell_dollar_volume_365d / NULLIF(total_buy_dollar_volume_365d + total_sell_dollar_volume_365d, 0) AS pct_sell_dollar_volume_365d,

        -- Trade imbalance (1-365d)
        (total_buy_dollar_volume_1d - total_sell_dollar_volume_1d) / NULLIF(total_buy_dollar_volume_1d + total_sell_dollar_volume_1d, 0) AS trade_imbalance_1d,
        (total_buy_dollar_volume_7d - total_sell_dollar_volume_7d) / NULLIF(total_buy_dollar_volume_7d + total_sell_dollar_volume_7d, 0) AS trade_imbalance_7d,
        (total_buy_dollar_volume_30d - total_sell_dollar_volume_30d) / NULLIF(total_buy_dollar_volume_30d + total_sell_dollar_volume_30d, 0) AS trade_imbalance_30d,
        (total_buy_dollar_volume_90d - total_sell_dollar_volume_90d) / NULLIF(total_buy_dollar_volume_90d + total_sell_dollar_volume_90d, 0) AS trade_imbalance_90d,
        (total_buy_dollar_volume_180d - total_sell_dollar_volume_180d) / NULLIF(total_buy_dollar_volume_180d + total_sell_dollar_volume_180d, 0) AS trade_imbalance_180d,
        (total_buy_dollar_volume_365d - total_sell_dollar_volume_365d) / NULLIF(total_buy_dollar_volume_365d + total_sell_dollar_volume_365d, 0) AS trade_imbalance_365d

    FROM rolling
),
latest AS (
    SELECT * EXCLUDE(rn)
    FROM (
        SELECT 
            *,
            row_number() OVER (PARTITION BY symbol_id ORDER BY date DESC) AS rn
        FROM feat
    )
    WHERE rn = 1
)
COPY (SELECT * FROM latest)
TO {out_dir} (
    FORMAT PARQUET,
    COMPRESSION 'SNAPPY',
    PARTITION_BY (symbol_id, date),
    WRITE_PARTITION_COLUMNS true,
    OVERWRITE
);
"""
# ----------------------- Execution helper ---------------------------------

def _run_duckdb_rolling(exec_date, input_dir: str, output_dir: str):
    cutoff = (exec_date - timedelta(days=364)).date()
    duckdb.execute(_SQL_TEMPLATE, {'input_dir': input_dir, 'out_dir': output_dir, 'cutoff': cutoff})

# ----------------------- DAG: FUTURES -------------------------------------
with DAG(
    dag_id="futures_trade_features_rolling_1d",
    catchup=False,
) as dag:
    compute = PythonOperator(
        task_id="calc_futures_trade_rollups",
        python_callable= _run_duckdb_rolling,
        op_kwargs={
            'exec_date': "{{ ds }}",
            'input_dir': AGG_FUTURES_TRADES.uri,
            'output_dir': ROLLING_FUTURES_TRADES.uri,
        },
    )
    finish = EmptyOperator(task_id="finish")
    compute >> finish
