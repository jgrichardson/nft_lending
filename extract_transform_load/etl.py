# Import Libraries
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
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


load_dotenv()

rarify_api_key = os.getenv("RARIFY_API_KEY")

network_id = "ethereum"

# Crypto Punks
contract_id = "ethereum:b47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

# Time frame of data pull
period = "90d"

# Get list of top 100 contracts by highest volume
contracts_url = f"https://api.rarify.tech/data/contracts/?page[limit]=100&sort=-insights.volume"

# Get list of tokens
tokens_url = f"https://api.rarify.tech/data/tokens/?filter[contract]=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d"


# Get list of whales that own the specified contract
#whales_id = "ethereum:0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"
whales_id = "ethereum:dbfd76af2157dc15ee4e57f3f942bb45ba84af24"
whales_url = f"https://api.rarify.tech/data/contracts/{whales_id}/whales"


# Get the trade data for a specific contract from the past period
trades_url = f"https://api.rarify.tech/data/contracts/{contract_id}/insights/{period}"


# Use the following code to target a specific token in the collection
token_id = 9620
token_baseurl = f"https://api.rarify.tech/data/tokens/{network_id}:{contract_id}:{token_id}"


# Make API request call to Rarify
json_response = api_request(tokens_url, rarify_api_key)

# Serial json data
json_serialized = json.dumps(json_response, indent = 4)

# Output json data to a file
with open('response.json', 'w') as f:
    f.write(json_serialized)

# Display response in json format
print(json_serialized)

###
#
# START HERE...
# Create dataframe for contract and trades from API call.  Then store the data into DB.
#
###



