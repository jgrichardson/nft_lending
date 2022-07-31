# Import Libraries
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import inspect
import requests
import db_utils as db
import logging


def fetch_rarify_data(url, key):
    """
    The following function is our base fetch for the collection data using our authorization key stored in the environment
    variables as well as the url that we supply to the function
    The url must be supplied with a valid network_id, contract_id, and token_id
    The function returns the sale_history_data for our targeted collection at the 'history' endpoint
    """
    sale_history_data = requests.get(
        url,
        headers={"Authorization": f"Bearer {key}"}
    ).json()
    #display(sale_history_data)
    return sale_history_data['included'][1]['attributes']['history']


load_dotenv()

rarify_api_key = os.getenv("RARIFY_API_KEY")
print(type(rarify_api_key))


network_id = "ethereum"
# Crypto Punks
contract_id = "b47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

# Time frame of data pull
period = "90d"

collections_baseurl = f"https://api.rarify.tech/data/contracts/{network_id}:{contract_id}/insights/{period}"

# Use the following code to target a specific token in the collection
token_id = 9620
token_baseurl = f"https://api.rarify.tech/data/tokens/{network_id}:{contract_id}:{token_id}"


punks_return = fetch_rarify_data(collections_baseurl, rarify_api_key)
punks_df = pd.DataFrame(punks_return)
punks_df['time'] = pd.to_datetime(punks_df['time'], infer_datetime_format=True)
punks_df = punks_df.set_index('time')
punks_df.head()

convert_dict = {'avg_price': float,
                'max_price': float,
                'min_price': float,
                'trades': int,
                'unique_buyers': int,
                'volume':  float,
               }  
  
punks_df = punks_df.astype(convert_dict)  
punks_df[['avg_price', 'max_price', 'min_price', 'volume']] = punks_df[['avg_price', 'max_price', 'min_price', 'volume']] * 10**-18
punks_df.head()



