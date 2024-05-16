# Data Pipelines
Repository containing the data pipelines for my trading bot.  Repository has the following file structure:
```
.
├── README.md
├── dags
│   ├── fetch_eth_block_rewards.py
│   ├── fetch_eth_transaction_gas_used.py
│   ├── fetch_new_eth_data.py
│   ├── fetch_order_book_data_1h.py
│   ├── fetch_tick_data.py
│   ├── fetch_token_prices_1h.py
├── plugins
    ├── __init__.py
    ├── operators
        ├── __init__.py
        ├── get_block_rewards.py
        ├── get_coinapi_prices_operator.py
        ├── get_eth_transaction_gas_used.py
        ├── get_order_book_data_operator.py
        ├── get_tick_data_operator.py
        ├── redshift_sql_operator.py
        └── web3_alchemy_to_s3_operator.py

3 directories, 16 files, 2080 lines of code
```
