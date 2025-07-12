# dags/datasets.py
from airflow.datasets import Dataset

# Local paths for testing (switch to S3 or other storage in production)

# Raw datasets
RAW_SPOT_TRADES   = Dataset("~/LocalData/data/trade_data/raw")
RAW_FUTURES_TRADES = Dataset("~/LocalData/data/futures_trade_data/raw")
RAW_SPOT_OHLCV   = Dataset("~/LocalData/data/ohlcv_data/raw")
RAW_FUTURES_OHLCV = Dataset("~/LocalData/data/futures_ohlcv_data/raw")

# Aggregated datasets
AGG_SPOT_OHLCV   = Dataset("~/LocalData/data/ohlcv_data/agg")
AGG_FUTURES_OHLCV = Dataset("~/LocalData/data/futures_ohlcv_data/agg")
AGG_SPOT_TRADES   = Dataset("~/LocalData/data/trade_data/agg")
AGG_FUTURES_TRADES = Dataset("~/LocalData/data/futures_trade_data/agg")

# trade feature datasets
FEATURE_SPOT_TRADES   = Dataset("~/LocalData/data/trade_data/features")
FEATURE_FUTURES_TRADES = Dataset("~/LocalData/data/futures_trade_data/features")

# ML feature datasets
ML_FEATURES = Dataset("~/LocalData/data/ml_features")