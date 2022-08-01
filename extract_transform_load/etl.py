# Import Libraries
from tokenize import String
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from psycopg2 import Timestamp
from sqlalchemy import BigInteger, create_engine
from sqlalchemy import inspect
import requests
import db_utils as db
import json
import logging



def api_request(url, key):    
    response_json = requests.get(
        url,
        headers={"Authorization": f"Bearer {key}"}
    ).json()
    return response_json



def get_contracts(obj_json):
    contracts_df = pd.DataFrame()    
    contracts_dict = obj_json['data']
    contracts_df = pd.DataFrame(contracts_dict)
    contracts_list = []
    for index, contract in contracts_df.iterrows():
        attr_dict = contract["attributes"]
        contracts_dict = {'contract_id' : contract['id'], 'address' : attr_dict.get('address'), 'name' : attr_dict.get('name'), 'description' : attr_dict.get('description'), 'external_url' : attr_dict.get('external_url'),
                          'network_id' : attr_dict.get('network'), 'primary_interface' : attr_dict.get('primary_interface'), 'royalties_fee_basic_points' : attr_dict.get('royalties_fee_basic_points'),
                          'royalties_receiver' : attr_dict.get('royalties_receiver'), 'num_tokens' : attr_dict.get('tokens'), 'unique_owners' : attr_dict.get('unique_owners')}
        contracts_list.append(contracts_dict)
    contract_df = pd.DataFrame(contracts_list)
    return contract_df




def get_tokens_by_contract_id(obj_json):
    tokens_df = pd.DataFrame()    
    tokens_dict = obj_json['data']
    tokens_df = pd.DataFrame(tokens_dict)
    tokens_list = []
    for index, token in tokens_df.iterrows():
        attr_dict = token["attributes"]
        tokens_dict = {'token_id' : token['id'], 'id_num' : attr_dict.get('token_id'), 'name' : attr_dict.get('name'), 'description' : attr_dict.get('description')}
        tokens_list.append(tokens_dict)
    token_df = pd.DataFrame(tokens_list)
    return token_df



def get_trades(obj_json):
    convert_dict = { 
                    "avg_price"     : 'float',
                    "max_price"     : 'float',
                    "min_price"     : 'float',
                    "time"          : 'object',
                    "trades"        : 'int64',
                    "unique_buyers" : 'int64',
                    "volume"        : 'float'
                   }  

    # There appears to be a bug in the Rarify response object.  Sometimes the history data is returned at index zero instead of 
    # index one in the included list.  Thus, I will swallow the exception here if that does occur and then use the correct index
    # and move one.  So far the keyerror 'history' error hasn't been happening.                      
    try:        
        trades_history = obj_json['included'][1]['attributes']['history']
    except Exception:
        trades_history = obj_json['included'][0]['attributes']['history']
        pass

    trades_df = pd.DataFrame(trades_history)
    trades_df["time"] = pd.to_datetime(trades_df["time"], infer_datetime_format=True)     
    trades_df = trades_df.astype(convert_dict)
    trades_df[["avg_price", "max_price", "min_price", "volume"]] = round(trades_df[["avg_price", "max_price", "min_price", "volume"]] * 10**-18, 2)
    return trades_df



load_dotenv()

rarify_api_key = os.getenv("RARIFY_API_KEY")

network_id = "ethereum"

# Time frame of data pull
period = "90d"


# Get list of top 100 contracts by highest volume
contracts_url = f"https://api.rarify.tech/data/contracts/?page[limit]=100&sort=-insights.volume"

# Make API request call to Rarify to get a list of ethereum contracts
# Store contract information.  Then loop through dataframe and 
# make API request call to Rarify again to get trades data for each contract
contracts_df = get_contracts(api_request(contracts_url, rarify_api_key))

# Make call to db.save_contracts() passing in a list of contracts 
db.save_contract(contracts_df)

# Loop through contracts and for each contract store information in db and get trade information
# for index, contract in contracts_df.iterrows():
contracts_list = contracts_df.contract_id.values.tolist()

"""
for contract_id in contracts_list:    
    # Get the trade data for a specific contract from the past period
    trades_url = f"https://api.rarify.tech/data/contracts/{contract_id}/insights/{period}"
    # Make API request call to Rarify to get trades data
    trades_df = get_trades(api_request(trades_url, rarify_api_key))
    trades_df["contract_id"] = contract_id
    trades_df["period"] = period
    trades_df["api_id"] = 'rarify'

    # Write trades dataframe to a .csv file
    #trades_df.to_csv("trades.csv", index = False)

    # Make call db.save_trade() passing in a list of trades history data per contract
    trades_df.set_index("time")
    db.save_trade(trades_df)
"""


for contract_id in contracts_list:
    # Get list of tokens
    tokens_url = f"https://api.rarify.tech/data/tokens/?filter[contract]={contract_id}"

    # Make API request call to Rarify
    tokens_df = get_tokens_by_contract_id(api_request(tokens_url, rarify_api_key))

    # Set contract_id for list of tokens retrieved
    tokens_df["contract_id"] = contract_id

    # Write token dataframe to a .csv file
    #tokens_df.to_csv("tokens.csv", index = False)

    # Make call db.save_token() passing in a list of tokens data per contract
    db.save_token(tokens_df)

    # Serial json data
    #json_serialized = json.dumps(json_response, indent = 4)

    # Output json data to a file
    #with open('token_response.json', 'w') as f:
    #     f.write(json_serialized)

    # Display response in json format
    #print(json_serialized)


# Get list of whales that own the specified contract
#whales_id = "ethereum:0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"
whales_id = "ethereum:dbfd76af2157dc15ee4e57f3f942bb45ba84af24"
whales_url = f"https://api.rarify.tech/data/contracts/{whales_id}/whales"


# Get the list of wallets either by ?filter[owner, contract, network, etc]=...
# Currently returning 404 so maybe the server is no longer up?
wallets_url = f"https://api.rarify.tech/data/wallets/?filter[network]=ethereum"

# Crypto Punks
contract_id = "ethereum:b47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

# Use the following code to target a specific token in the collection
token_id = 9620
token_baseurl = f"https://api.rarify.tech/data/tokens/{network_id}:{contract_id}:{token_id}"




