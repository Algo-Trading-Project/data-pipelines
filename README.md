# Data Pipelines
Repository containing the data pipelines for my trading bot.  Repository has the following file structure:
```
.
├── dags
│   ├── fetch_binance_futures_ohlcv_data.py
│   ├── fetch_binance_futures_trade_data.py
│   ├── fetch_binance_ohlcv_data.py
│   └── fetch_binance_trade_data.py
├── plugins
│   ├── __init__.py
│   └── operators
│       ├── __init__.py
│       ├── get_binance_futures_ohlcv_data_operator.py
│       ├── get_binance_futures_trade_data_operator.py
│       ├── get_binance_ohlcv_data_operator.py
│       ├── get_binance_order_book_data_operator.py
│       ├── get_binance_trade_data_operator.py
│       └── redshift_sql_operator.py
└── webserver_config.py

4 directories, 13 files
```
