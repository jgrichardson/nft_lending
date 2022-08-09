import pandas as pd
import os
# os.getenv
from dotenv import load_dotenv
import hvplot.pandas
import requests
from utils import *
from MCForecastTools import MCSimulation


load_dotenv()

rarify_api_key = os.getenv("RARIFY_API_KEY")

top_collections_baseurl = f"https://api.rarify.tech/data/contracts?include=insights&sort=-unique_buyers"

top_collections_return = fetch_top_collections_data(top_collections_baseurl, rarify_api_key)

del top_collections_return ['a310425046661c523d98344f7e9d66b32195365d']
del top_collections_return ['495f947276749ce646f68ac8c248420045cb7b5e']

collection_df = fetch_collections_data(top_collections_return, rarify_api_key)
coll_df = collection_df.copy()

cols = ["avg_price", "max_price", "min_price", "trades", "unique_buyers", "volume"]
new_cols = []
for key in top_collections_return.keys():
    for c in cols:
        new_cols.append(f"{key}_{c}")

contract_ids = {
                "bape": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", 
                "punks": "b47e3cd837ddf8e4c57f05d70ab865de6e193bbb", 
                "clonex": "0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B",
                "doodles": "0x8a90CAb2b38dba80c64b7734e58Ee1dB38B8992e",
                "neotokyo": "0xb668beB1Fa440F6cF2Da0399f8C28caB993Bdd65",
                "mfers": "0x79FCDEF22feeD20eDDacbB2587640e45491b757f",
}

# Store the resulting concatenated DataFrame in a sum_df object

sum_df = fetch_collections_data(contract_ids, rarify_api_key)

collection_df = sum_df.copy()

sim = MCSimulation(sum_df, num_simulation=100, num_trading_days=90)

cum_return = sim.calc_cumulative_return()

print(cum_return.hvplot())