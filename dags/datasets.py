from airflow.datasets import Dataset

# Local paths for testing (switch to S3 or other storage in production)

# Raw datasets
RAW_SPOT_OHLCV   = Dataset("~/LocalData/data/ohlcv_data/raw")
RAW_FUTURES_OHLCV = Dataset("~/LocalData/data/futures_ohlcv_data/raw")
RAW_SPOT_TRADES   = Dataset("~/LocalData/data/trade_data/raw")
RAW_FUTURES_TRADES = Dataset("~/LocalData/data/futures_trade_data/raw")

# Aggregated datasets
AGG_SPOT_OHLCV   = Dataset("~/LocalData/data/ohlcv_data/agg")
AGG_FUTURES_OHLCV = Dataset("~/LocalData/data/futures_ohlcv_data/agg")
AGG_SPOT_TRADES   = Dataset("~/LocalData/data/trade_data/agg")
AGG_FUTURES_TRADES = Dataset("~/LocalData/data/futures_trade_data/agg")

# Rolling trade feature datasets
ROLLING_SPOT_TRADES   = Dataset("~/LocalData/data/trade_data/rolling")
ROLLING_FUTURES_TRADES = Dataset("~/LocalData/data/futures_trade_data/rolling")

# ML feature datasets
FINAL_ML_FEATURES = Dataset("~/LocalData/data/ml_features")
