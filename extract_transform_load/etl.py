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



load_dotenv()

# Get Logger
logging.basicConfig(filename='etl.log', filemode='w', level=logging.DEBUG, format='%(levelname)s: %(asctime)s - %(message)s')
logger = logging.getLogger()

rarify_api_key = os.getenv("RARIFY_API_KEY")

# Time frame of data pull
#period = "all_time"
#period = "90d"
period = "90d"


# Limit the number of contracts to return i.e. default = 10
# num_contracts = 100
num_contracts = 100



def api_request(url, key):  
    response_json = ''
    try:
        response_json = requests.get(
            url,
            headers={"Authorization": f"Bearer {key}"}
        ).json()
    except Exception as ex:
        logger.debug(f"url: {url}, key: {key}")
        logger.debug(ex)
    return response_json



def get_contracts(obj_json):   

    # Initial Dataframe
    contract_df = pd.DataFrame()
    try:
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
    except Exception as ex:
        logger.debug(obj_json)
        logger.debug(ex)
        pass
    return contract_df



def get_smart_floor_price(obj_json):
    # Serial json data
    #json_serialized = json.dumps(obj_json, indent = 4)

    # Output json data to a file
    #with open('smart_response.json', 'w') as f:
    #    f.write(json_serialized)

    # Display response in json format
    #print(json_serialized)  
    try:        
        price = round(float(obj_json['data']['attributes']['price']) * 10**-18, 2)
    except Exception as ex:
        price = 0.00  
        logger.debug(obj_json)
        logger.debug(ex)
        pass    
    return price



def get_tokens_by_contract_id(obj_json):
    # Serial json data
    json_serialized = json.dumps(obj_json, indent = 4)

    # Output json data to a file
    with open('tokens_by_contract_response.json', 'w') as f:
        f.write(json_serialized)

    # Display response in json format
    #print(json_serialized)  
    # Initial Dataframe
    token_df = pd.DataFrame()
    try:
        tokens_dict = obj_json['data']
        tokens_df = pd.DataFrame(tokens_dict)
        # Create empty list
        tokens_list = []        
        for index, token in tokens_df.iterrows():
            attr_dict = token["attributes"]
            tokens_dict = {'token_id' : token['id'], 'id_num' : attr_dict.get('token_id'), 'name' : attr_dict.get('name'), 'description' : attr_dict.get('description') }
            tokens_list.append(tokens_dict)
        token_df = pd.DataFrame(tokens_list)            
    except Exception as ex:
        logger.debug(obj_json['data'])
        logger.debug(ex)
        pass
    return token_df



def get_token_attributes(obj_json):
    # Serial json data
    json_serialized = json.dumps(obj_json, indent = 4)

    # Output json data to a file
    with open('token_attributes_response.json', 'w') as f:
        f.write(json_serialized)

    # Display response in json format
    #print(obj_json['included']) 

    # Initialize Dataframe and Array
    token_df = pd.DataFrame()
    try:        
        token_attributes_dict = obj_json['included'][0]['attributes']['attributes_stats']
        if token_attributes_dict:
            token_attributes_list = []            
            for token_attribute in token_attributes_dict:
                token_attributes_dict = {'overall_with_trait_value' : token_attribute['overall_with_trait_value'], 'rarity_percentage' : token_attribute['rarity_percentage'], 'trait_type' : token_attribute['trait_type'], 'value' : token_attribute['value']}
                token_attributes_list.append(token_attributes_dict)
            token_df = pd.DataFrame(token_attributes_list)        
    except Exception as ex:
        logger.debug(obj_json['included'])
        logger.debug(ex)
        pass
    return token_df



def get_trades(obj_json):
    # Serial json data
    json_serialized = json.dumps(obj_json, indent = 4)

    # Output json data to a file
    with open('trades_response.json', 'w') as f:
        f.write(json_serialized)

    # Display response in json format
    #print(json_serialized)  
    # Initialize Dataframe
    trades_df = pd.DataFrame()

    # Create empty dictionary
    trades_history = {}
    # There appears to be a bug in the Rarify response object.  Sometimes the history data is returned at index zero instead of 
    # index one in the included list.  Thus, I will swallow the exception here if that does occur and then use the correct index
    # and move one.  So far the keyerror 'history' error hasn't been happening.                      
    try:        
        trades_history = obj_json['included'][1]['attributes']['history']
    except Exception as ex:
        trades_history = obj_json['included'][0]['attributes']['history']     
        logger.debug(ex)
        pass    
    try:
        if trades_history:
            convert_dict = { 
                            "avg_price"     : 'float',
                            "max_price"     : 'float',
                            "min_price"     : 'float',
                            "time"          : 'object',
                            "trades"        : 'int64',
                            "unique_buyers" : 'int64',
                            "volume"        : 'float'
                        }          
            trades_df = pd.DataFrame(trades_history)
            trades_df["time"] = pd.to_datetime(trades_df["time"], infer_datetime_format=True)     
            trades_df = trades_df.astype(convert_dict)
            trades_df[["avg_price", "max_price", "min_price", "volume"]] = round(trades_df[["avg_price", "max_price", "min_price", "volume"]] * 10**-18, 2)
    except Exception as ex:
        logger.debug(obj_json['included'])
        logger.debug(ex)
        pass
    return trades_df



# Get list of top 100 contracts by highest volume
contracts_url = f"https://api.rarify.tech/data/contracts/?page[limit]={num_contracts}&sort=-insights.volume"


# Make API request call to Rarify to get a list of top 100 collections
contracts_df = get_contracts(api_request(contracts_url, rarify_api_key))

if not contracts_df.empty:
    # Get a list of contract_ids from the collection
    #contracts_list = contracts_df.contract_id.values.tolist()

    # Use the following code to obtain the smart floor price in the collection
    smart_floor_url = f"https://api.rarify.tech/data/contracts/contract_id/smart-floor-price"

    # Make API request call to Rarify to get the smart floor price for each collection of what
    # an NFT's floor price within the collection would sell for in the open market.
    contracts_df['smart_floor_price'] = [get_smart_floor_price(api_request(smart_floor_url.replace('contract_id', i), rarify_api_key)) for i in contracts_df['contract_id']]

    # Make call to db.save_contract() passing in a list of contracts 
    # and store the data in the database
    db.save_contract(contracts_df)

    # Get a list of contract_ids from the collection
    contracts_list = contracts_df.contract_id.values.tolist()

    # Loop through contracts and get trade information and store data for each contract id
    for contract_id in contracts_list:    
        # Get the trade data for a specific contract from the past period
        trades_url = f"https://api.rarify.tech/data/contracts/{contract_id}/insights/{period}"
        # Make API request call to Rarify to get trades data
        trades_df = get_trades(api_request(trades_url, rarify_api_key))

        if not trades_df.empty:
            trades_df["contract_id"] = contract_id
            trades_df["period"] = period
            trades_df["type"] = "collection"
            trades_df["api_id"] = 'rarify'
            # Make call db.save_trade() passing in a list of trades history data per contract
            trades_df.set_index("time")
            db.save_trade(trades_df)

    # Loop through contracts and get tokens associated with the collection.  Then store
    # the data for each token.
    for contract_id in contracts_list:
        # Get list of tokens
        tokens_url = f"https://api.rarify.tech/data/tokens/?filter[contract]={contract_id}"

        # Make API request call to Rarify
        tokens_df = get_tokens_by_contract_id(api_request(tokens_url, rarify_api_key))

        if not tokens_df.empty:
            # Set contract_id for list of tokens retrieved
            tokens_df["contract_id"] = contract_id
            # Make call to db.save_token(df) passing in a dataframe of tokens per contract
            db.save_token(tokens_df)

            # Get a list of token_ids from the list of tokens
            tokens_list = tokens_df.token_id.values.tolist()

            for token_id in tokens_list:
                # Make API request call to Rarify to get token attributes i.e. the rarity percentage, the overall trait value, trait_type, etc. per coin
                token_url = f"https://api.rarify.tech/data/tokens/{token_id}/?include=attributes_stats"
                token_attributes_df = get_token_attributes(api_request(token_url, rarify_api_key))

                if not token_attributes_df.empty:     
                    token_attributes_df["token_id"] = token_id  
                    # Make call to db.save_token_attributes() passing in a dataframe of token attributes per token
                    db.save_token_attributes(token_attributes_df)

                # Make API request call to Rarify to get trades per token
                trade_url = f"https://api.rarify.tech/data/tokens/{token_id}/insights/{period}"
                trades_df = get_trades(api_request(trade_url, rarify_api_key))
                
                if not trades_df.empty:
                    trades_df["contract_id"] = token_id
                    trades_df["period"] = period
                    trades_df["type"] = "token"
                    trades_df["api_id"] = 'rarify'
                    trades_df.set_index("time")
                    # Make call db.save_trade() passing in a list of trades history data per token                    
                    db.save_trade(trades_df)        

# Get list of whales that own the specified contract
#whales_id = "ethereum:0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"
whales_id = "ethereum:dbfd76af2157dc15ee4e57f3f942bb45ba84af24"
whales_url = f"https://api.rarify.tech/data/contracts/{whales_id}/whales"

# Get the list of wallets either by ?filter[owner, contract, network, etc]=...
# Currently returning 404 so maybe the server is no longer up?
wallets_url = f"https://api.rarify.tech/data/wallets/?filter[network]=ethereum"





