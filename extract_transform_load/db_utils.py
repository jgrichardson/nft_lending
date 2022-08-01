import pandas as pd
import math
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import inspect

# Load .env environment variables
load_dotenv()

# Retrieve the database settings from .env file
database_connection_string = os.getenv("DATABASE_URI")

# Retrieve the database schema from .env file
database_schema = os.getenv("DATABASE_SCHEMA")

# Create a database connection
engine = create_engine(database_connection_string, echo = False)


def get_all_table_names():
    """
    This function returns a list of all the existing tables

    Returns: List
    """
    inspector = inspect(engine)
    tables = inspector.get_table_names(database_schema)
    return tables



def scrub_str(str):
    """
    This function checks for none objects as well as cleans up any characters which
    would result into an exception being thrown

    Args: str - object to analyze
    Returns: string object
    """
    if str is not None:
        if str.isdigit():
            return str(str)
        str = re.sub(r"[\([{'})\]]", "", str)
        str = re.sub(r"%", "pct.", str)
        return str
    else:
        return ''




def scrub_int(num):
    """
    This function checks for none objects 

    Args: num - numeric object to analyze
    Returns: number
    """
    if num is not None:
        if math.isnan(num):
            return 0
        if num == 0.0:
            return 0
        return num
    else:
        return 0



"""

    CRUD Operations for the Trades table

"""
def get_all_trades(contract_id):
    """
    This function retrieves all the trades for a specific contract  

    Args: contract_id - collection's contract id
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.trade   
    WHERE contract_id = '{contract_id}'
    """    
    df = pd.read_sql_query(sql_query, con = engine)    
    return df


def get_trade(contract_id, time):
    """
    This function retrieves a specific trade information
    Check by both contract_id and time

    Args: contract_id - a collection's contract id
          time - the timestamp when the trade was made
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.trade   
    WHERE contract_id = '{contract_id}'
    AND timestamp = '{time}'
    """        
    df = pd.read_sql_query(sql_query, con = engine)                
    return df


def delete_trade(contract_id, time):
    """
    This function deletes a specific trade  
    Check by both contract_id and time

    Args: contract_id - a collection's contract id
          time - the timestamp when the trade was made
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.trade   
    WHERE contract_id = '{contract_id}'
    AND timestamp = '{time}'
    """    
    with engine.connect() as conn:
        conn.execute(delete_query)
        print(f"The trade for {contract_id} at {time} was successfully deleted!")


def check_if_trade_exists(contract_id, time):
    """
    This function calls load_trade to check if the trade data already exists.  
    
    Args: contract_id - a collection's contract id
          time - the timestamp when the trade was made
    Returns: Boolean
    """        
    df = get_trade(contract_id, time)
    if len(df.index) > 0:
        return True        
    return False
    

def update_trade(df):
    """
    This function updates the trade table
    
    Args: df - data collection of trades
    """
    update_query = f"""
    UPDATE {database_schema}.trade
    SET avg_price  = {round(df['avg_price'], 2)},
        max_price  = {round(df['max_price'], 2)},
        min_price  = {round(df['min_price'], 2)},
        num_trades = {df['trades']},
        unique_buyers = {df['unique_buyers']},
        volume = {round(df['volume'], 2)},
        period = '{df['period']}',
        api_id = '{df['api_id']}'
    WHERE contract_id = '{df['contract_id']}'
    AND timestamp = '{df['time']}'
    """    
    with engine.connect() as conn:
        conn.execute(update_query)


def insert_trade(df):
    """
    This function inserts a new trade
    
    Args: df - data collection of trades
    """    
    insert_query = f"""
    INSERT INTO {database_schema}.trade (contract_id, timestamp, avg_price, max_price, min_price, num_trades, unique_buyers, volume, period, api_id)
    VALUES ('{df['contract_id']}', '{df['time']}', {round(df['avg_price'], 2)}, {round(df['max_price'], 2)}, {round(df['min_price'], 2)}, {df['trades']}, {df['unique_buyers']}, {round(df['volume'], 2)}, '{df['period']}', '{df['api_id']}')
    """    
    with engine.connect() as conn:
        conn.execute(insert_query)
    

def save_trade(df):
    """
    This function saves the collection data into a postgres database residing in AWS
    
    Args: df - data collection of trades
    """    
    for row_index in df.index: 
        # First check if the trade exists
        trade_exists = check_if_trade_exists(df.iloc[row_index]['contract_id'], df.iloc[row_index]['time'])
        
        # If the trade exists then we update the information.  Otherwise, we add a new trade
        if trade_exists:
            update_trade(df.iloc[row_index])
        else:
            insert_trade(df.iloc[row_index])                    



"""

    CRUD Operations for the Contracts table

"""
def get_all_contracts():
    """
    This function returns a list of all the contracts

    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.contract   
    """    
    df = pd.read_sql_query(sql_query, con = engine)    
    return df


def get_contract(contract_id):
    """
    This function returns information for a specific contract    

    Args: contract_id - a collection's contract id
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.contract   
    WHERE contract_id = '{contract_id}'
    """        
    df = pd.read_sql_query(sql_query, con = engine)                
    return df


def delete_contract(contract_id):
    """
    This function deletes a specific contract

    Args: contract_id - a collection's contract id
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.contract   
    WHERE contract_id = '{contract_id}'
    """    
    with engine.connect() as conn:
        conn.execute(delete_query)
        print(f"{contract_id} was successfully deleted!")


def check_if_contract_exists(contract_id):
    """
    This function calls get_contract to check if the contract already exists.  
    
    Args: contract_id - a collection's contract id
    Returns: Boolean
    """        
    df = get_contract(contract_id)
    if len(df.index) > 0:
        return True        
    return False
    

def update_contract(contract_id, df):
    """
    This function updates the contract information
    
    Args: contract_id - a collection's contract id
          df - data collection of contract data
    """
    name = scrub_str(df['name'])
    descr = scrub_str(df['description'])
    royalties_fee_basic_points = scrub_int(df["royalties_fee_basic_points"])
    royalties_receiver = scrub_str(df['royalties_receiver'])

    update_query = f"""
    UPDATE {database_schema}.contract
    SET address = '{df['address']}',
        name  = '{name}',
        description = '{descr}',        
        external_url = '{df['external_url']}',
        network_id = '{df['network_id']}',
        primary_interface = '{df['primary_interface']}',
        royalties_fee_basic_points = {royalties_fee_basic_points},        
        royalties_receiver = '{royalties_receiver}',
        num_tokens = {df['num_tokens']},
        unique_owners = {df['unique_owners']}
    WHERE contract_id = '{contract_id}'
    """       
    try:
        with engine.connect() as conn:
            conn.execute(update_query)
    except Exception as ex:
        print(f"Exception caught: {ex}")
        print(update_query)       


def insert_contract(contract_id, df):
    """
    This function inserts a new contract
    
    Args: contract_id - a collection's contract id
          df - data collection of trades
    """   
    name = scrub_str(df['name'])
    descr = scrub_str(df['description'])
    royalties_fee_basic_points = scrub_int(df["royalties_fee_basic_points"])
    royalties_receiver = scrub_str(df['royalties_receiver'])

    insert_query = f"""
    INSERT INTO {database_schema}.contract (contract_id, address, name, description, external_url, network_id, primary_interface, royalties_fee_basic_points, royalties_receiver, num_tokens, unique_owners)
    VALUES ('{contract_id}', '{df['address']}', '{name}', '{descr}', '{df['external_url']}', '{df['network_id']}', '{df['primary_interface']}', {royalties_fee_basic_points}, '{royalties_receiver}', {df['num_tokens']}, {df['unique_owners']})
    """        
     
    try:
        with engine.connect() as conn:
            conn.execute(insert_query)
    except Exception as ex:
        print(f"Exception caught: {ex}")    
        print(insert_query)
    

def save_contract(contract_df):
    """
    This function saves the contract data into a postgres database residing in AWS
    
    Args: df - data collection of contracts
    """    
    for row_index in contract_df.index: 
        contract_id = contract_df['contract_id'][row_index]
        # First check if the contract exists
        contract_exists = check_if_contract_exists(contract_id)

        # If the contract exists then we update the information.  Otherwise, we add a new contract
        if contract_exists:
            update_contract(contract_id, contract_df.iloc[row_index])
        else:
            insert_contract(contract_id, contract_df.iloc[row_index])                                



"""

    CRUD Operations for the Networks table

"""
def get_all_networks():
    """
    This function returns a list of all the networks

    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.network   
    """    
    df = pd.read_sql_query(sql_query, con = engine)    
    return df


def get_network(network_id):
    """
    This function returns information for a specific network 

    Args: network_id - the id of a specific blockchain
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.network   
    WHERE network_id = '{network_id}'
    """        
    df = pd.read_sql_query(sql_query, con = engine)                
    return df


def delete_network(network_id):
    """
    This function deletes a specific network

    Args: network_id - the id of a specific blockchain
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.network   
    WHERE network_id = '{network_id}'
    """    
    with engine.connect() as conn:
        conn.execute(delete_query)
        print(f"{network_id} was successfully deleted!")


def check_if_network_exists(network_id):
    """
    This function calls get_network to check if the network already exists.  
    
    Args: network_id - the id of a specific blockchain
    Returns: Boolean
    """        
    df = get_network(network_id)
    if len(df.index) > 0:
        return True        
    return False
    

def update_network(network_id, df):
    """
    This function updates the network information
    
    Args: network_id - the id of a specific blockchain
          df - data collection of network data
    """
    update_query = f"""
    UPDATE {database_schema}.network
    SET short_name  = '{df['short_name']}',
        network_id = '{df['network_id']}'
    WHERE network_id = '{network_id}'
    """    
    with engine.connect() as conn:
        conn.execute(update_query)


def insert_network(network_id, df):
    """
    This function inserts a new network
    
    Args: network_id - the id of a specific blockchain
          df - data collection of networks
    """    
    insert_query = f"""
    INSERT INTO {database_schema}.network (network_id, short_name)
    VALUES ('{network_id}', '{df['short_name']}')
    """    
    with engine.connect() as conn:
        conn.execute(insert_query)
    

def save_network(network_id, df):
    """
    This function saves the network data into a postgres database residing in AWS
    
    Args: network_id - the id of a specific blockchain
          df - data collection of networks
    """    
    for row_index in df.index: 
        # First check if the network exists
        network_exists = check_if_network_exists(network_id)
        
        # If the network exists then we update the information.  Otherwise, we add a new network
        if network_exists:
            update_network(network_id, df.iloc[row_index])
        else:
            insert_network(network_id, df.iloc[row_index])                                



"""

    CRUD Operations for the APIs table

"""
def get_all_api_requests():
    """
    This function returns a list of all the apis

    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.api   
    """    
    df = pd.read_sql_query(sql_query, con = engine)    
    return df


def get_api_request(api_id):
    """
    This function returns information for a specific api

    Args: api_id - the id of the api request
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.api   
    WHERE api_id = '{api_id}'
    """        
    df = pd.read_sql_query(sql_query, con = engine)                
    return df


def delete_api_request(api_id):
    """
    This function deletes a specific api request

    Args: api_id - the id of the api request
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.api   
    WHERE api_id = '{api_id}'
    """    
    with engine.connect() as conn:
        conn.execute(delete_query)
        print(f"{api_id} was successfully deleted!")


def check_if_api_exists(api_id):
    """
    This function calls get_api to check if the api request already exists.  
    
    Args: api_id - the id of a specific api request
    Returns: Boolean
    """        
    df = get_api_request(api_id)
    if len(df.index) > 0:
        return True        
    return False
    

def update_api_request(api_id, df):
    """
    This function updates the api information
    
    Args: api_id - the id of a specific api request
          df - data collection of network data
    """
    update_query = f"""
    UPDATE {database_schema}.api
    SET name  = '{df['name']}',
        endpoint = '{df['endpoint_url']}'
    WHERE api_id = '{api_id}'
    """    
    with engine.connect() as conn:
        conn.execute(update_query)


def insert_api_request(api_id, df):
    """
    This function inserts a new api request
    
    Args: api_id - the id of a specific api
          df - data collection of networks
    """    
    insert_query = f"""
    INSERT INTO {database_schema}.api (api_id, name, endpoint_url)
    VALUES ('{api_id}', '{df['name']}', '{df['endpoint_url']}')
    """    
    with engine.connect() as conn:
        conn.execute(insert_query)
    

def save_api_request(api_id, df):
    """
    This function saves the api request into a postgres database residing in AWS
    
    Args: api_id - the id of a specific api
          df - data collection of networks
    """    
    for row_index in df.index: 
        # First check if the api exists
        api_exists = check_if_api_exists(api_id)
        
        # If the api request exists then we update the information.  Otherwise, we add a new api request
        if api_exists:
            update_api_request(api_id, df.iloc[row_index])
        else:
            insert_api_request(api_id, df.iloc[row_index])                                



"""

    CRUD Operations for the Whales table

"""
def get_all_whales():
    """
    This function returns a list of all the whales

    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.whale  
    """    
    df = pd.read_sql_query(sql_query, con = engine)    
    return df


def get_whale(wallet_id):
    """
    This function returns information for a specific whale

    Args: wallet_id - a whale's wallet address
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.whale  
    WHERE network_id = '{wallet_id}'
    """        
    df = pd.read_sql_query(sql_query, con = engine)                
    return df


def delete_whale(wallet_id):
    """
    This function deletes a specific whale

    Args: wallet_id - a whale's wallet address
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.whale  
    WHERE wallet_id = '{wallet_id}'
    """    
    with engine.connect() as conn:
        conn.execute(delete_query)
        print(f"{wallet_id} was successfully deleted!")


def check_if_whale_exists(wallet_id):
    """
    This function calls get_whale to check if the whale already exists.  
    
    Args: wallet_id - a whale's wallet address
    Returns: Boolean
    """        
    df = get_whale(wallet_id)
    if len(df.index) > 0:
        return True        
    return False
    

def update_whale(wallet_id, df):
    """
    This function updates the whale's information
    
    Args: wallet_id - a whale's wallet address
          df - data collection of whale's data
    """
    update_query = f"""
    UPDATE {database_schema}.whale
    SET contract_id  = '{df['contract_id']}',
    WHERE wallet_id = '{wallet_id}'
    """    
    with engine.connect() as conn:
        conn.execute(update_query)


def insert_whale(wallet_id, df):
    """
    This function inserts a new whale
    
    Args: wallet_id - a whale's wallet address
          df - data collection of networks
    """    
    insert_query = f"""
    INSERT INTO {database_schema}.whale (wallet_id, contract_id)
    VALUES ('{wallet_id}', '{df['contract_id']}')
    """    
    with engine.connect() as conn:
        conn.execute(insert_query)
    

def save_the_whales(wallet_id, df):
    """
    This function saves the whale's data into a postgres database residing in AWS
    
    Args: wallet_id - a whale's wallet address
          df - data collection of the whale's information
    """    
    for row_index in df.index: 
        # First check if the whale exists
        whale_exists = check_if_whale_exists(wallet_id)
        
        # If the whale exists then we update the information.  Otherwise, we add a new whale
        if whale_exists:
            update_whale(wallet_id, df.iloc[row_index])
        else:
            insert_whale(wallet_id, df.iloc[row_index])                                



"""

    CRUD Operations for the Contract Maps table

"""
def get_all_contract_maps():
    """
    This function returns a list of all the contracts mapped

    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.contract_map  
    """    
    df = pd.read_sql_query(sql_query, con = engine)    
    return df


def get_contract_maps(contract_id):
    """
    This function returns information for a specific contract mapping  

    Args: contract_id - a collection's contract id
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.contract_map   
    WHERE contract_id = '{contract_id}'
    """        
    df = pd.read_sql_query(sql_query, con = engine)                
    return df


def delete_contract_maps(contract_id):
    """
    This function deletes a specific contract mapping

    Args: contract_id - a collection's contract id
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.contract_map  
    WHERE contract_id = '{contract_id}'
    """    
    with engine.connect() as conn:
        conn.execute(delete_query)
        print(f"{contract_id} was successfully deleted!")


def check_if_contract_map_exists(contract_id):
    """
    This function calls get_contract_maps to check if the contract mapping already exists.  
    
    Args: contract_id - a collection's contract id
    Returns: Boolean
    """        
    df = get_contract_maps(contract_id)
    if len(df.index) > 0:
        return True        
    return False
    

def update_contract_map(contract_id, df):
    """
    This function updates the contract mapping information
    
    Args: contract_id - a collection's contract id
          df - data collection of contract data
    """
    update_query = f"""
    UPDATE {database_schema}.contract_map
    SET   new_contract_id = {df['new_contract_id']}
    WHERE contract_id = '{contract_id}'
    """    
    with engine.connect() as conn:
        conn.execute(update_query)


def insert_contract_map(contract_id, df):
    """
    This function inserts a new contract mapping
    
    Args: contract_id - a collection's contract id
          df - data collection of trades
    """    
    insert_query = f"""
    INSERT INTO {database_schema}.contract_map (contract_id, new_contract_id)
    VALUES ('{contract_id}', '{df['new_contract_id']}')
    """    
    with engine.connect() as conn:
        conn.execute(insert_query)
    

def save_contract_maps(contract_id, df):
    """
    This function saves the contract mapping data into a postgres database residing in AWS
    
    Args: contract_id - a collection's contract id
          df - data collection of trades
    """    
    for row_index in df.index: 
        # First check if the contract mapping exists
        contract_map_exists = check_if_contract_map_exists(contract_id)
        
        # If the contract mapping exists then we update the information.  Otherwise, we add a new contract mapping
        if contract_map_exists:
            update_contract_map(contract_id, df.iloc[row_index])
        else:
            insert_contract_map(contract_id, df.iloc[row_index])                                



"""

    CRUD Operations for the Tokens table

"""
def get_all_tokens():
    """
    This function returns a list of all the tokens per a contract

    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.token  
    """    
    df = pd.read_sql_query(sql_query, con = engine)    
    return df


def get_token(token_id):
    """
    This function returns information for a specific token  

    Args: token_id - a token thats part of a contract i.e. Collection
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.token  
    WHERE token_id = '{token_id}'
    """        
    df = pd.read_sql_query(sql_query, con = engine)                
    return df


def delete_token(token_id):
    """
    This function deletes a specific token

    Args: token_id - a token thats part of a contract i.e. Collection
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.token  
    WHERE token_id = '{token_id}'
    """    
    with engine.connect() as conn:
        conn.execute(delete_query)
        print(f"{token_id} was successfully deleted!")


def check_if_token_exists(token_id):
    """
    This function calls get_token to check if the token already exists.  
    
    Args: token_id - a token thats part of a contract i.e. Collection
    Returns: Boolean
    """        
    df = get_token(token_id)
    if len(df.index) > 0:
        return True        
    return False
    

def update_token(token_id, df):
    """
    This function updates the token information
    
    Args: token_id - a token thats part of a contract i.e. Collection
          df - data collection of token data
    """
    update_query = f"""
    UPDATE {database_schema}.token
    SET id_number = '{df['id_number']}',
        name  = '{df['name']}',
        description = '{df['description']}',
        contract_id = '{df['contract_id']}'
    WHERE token_id = '{token_id}'
    """    
    with engine.connect() as conn:
        conn.execute(update_query)


def insert_token(token_id, df):
    """
    This function inserts a new token
    
    Args: token_id - a token thats part of a contract i.e. Collection
          df - data collection of trades
    """    
    insert_query = f"""
    INSERT INTO {database_schema}.token (token_id, id_number, name, description, contract_id)
    VALUES ('{token_id}', '{df['id_number']}', '{df['name']}', '{df['description']}', '{df['contract_id']}')
    """    
    with engine.connect() as conn:
        conn.execute(insert_query)
    

def save_token(token_df):
    """
    This function saves the token data into a postgres database residing in AWS
    
    Args: df - data collection of tokens thats part of a specific contract i.e. Collection
    """    
    for row_index in token_df.index: 
        token_id = token_df['token_id'][row_index]
        # First check if the token exists
        token_exists = check_if_token_exists(token_id)
        
        # If the token exists then we update the information.  Otherwise, we add a new token
        if token_exists:
            update_token(token_id, token_df.iloc[row_index])
        else:
            insert_token(token_id, token_df.iloc[row_index])                                
