import pandas as pd
import math
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import inspect
import logging

# Get Logger
logging.basicConfig(filename='db_utils.log', filemode='w', level=logging.DEBUG, format='%(levelname)s: %(asctime)s - %(message)s')
logger = logging.getLogger()

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
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names(database_schema)
        return tables
    except Exception as ex:    
        logger.error(ex)  



def scrub_str(str):
    """
    This function checks for none objects as well as cleans up any characters which
    would result into an exception being thrown

    Args: str - object to analyze
    Returns: string object
    """
    if str is not None:
        #if str.isdigit():
        #    return str(str)
        str = re.sub(r"[\([{'})\]]", "", str)
        str = re.sub(r"%", " pct.", str)
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
    try:
        df = pd.read_sql_query(sql_query, con = engine)    
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex)      


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
    try:        
        df = pd.read_sql_query(sql_query, con = engine)                
        return df
    except Exception as ex: 
        logger.debug(sql_query)   
        logger.error(ex)  


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
    try:  
        with engine.connect() as conn:
            conn.execute(delete_query)
            print(f"The trade for {contract_id} at {time} was successfully deleted!")
    except Exception as ex:   
        logger.debug(delete_query) 
        logger.error(ex)  


def check_if_trade_exists(contract_id, time):
    """
    This function calls load_trade to check if the trade data already exists.  
    
    Args: contract_id - a collection's contract id
          time - the timestamp when the trade was made
    Returns: Boolean
    """  
    try:      
        df = get_trade(contract_id, time)
        if len(df.index) > 0:
            return True        
        return False
    except Exception as ex:    
        logger.error(ex)      


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
        type = '{df['type']}',
        api_id = '{df['api_id']}'
    WHERE contract_id = '{df['contract_id']}'
    AND timestamp = '{df['time']}'
    """
    try:    
        with engine.connect() as conn:
            conn.execute(update_query)
    except Exception as ex: 
        logger.debug(update_query)   
        logger.error(ex)              


def insert_trade(df):
    """
    This function inserts a new trade
    
    Args: df - data collection of trades
    """    
    insert_query = f"""
    INSERT INTO {database_schema}.trade (contract_id, timestamp, avg_price, max_price, min_price, num_trades, unique_buyers, volume, period, type, api_id)
    VALUES ('{df['contract_id']}', '{df['time']}', {round(df['avg_price'], 2)}, {round(df['max_price'], 2)}, {round(df['min_price'], 2)}, {df['trades']}, {df['unique_buyers']}, {round(df['volume'], 2)}, '{df['period']}', '{df['type']}', '{df['api_id']}')
    """  
    try:  
        with engine.connect() as conn:
            conn.execute(insert_query)
    except Exception as ex:  
        logger.debug(insert_query)  
        logger.error(ex)      


def save_trade(df):
    """
    This function saves the collection data into a postgres database residing in AWS
    
    Args: df - data collection of trades
    """    
    for row_index in df.index:      
        # Write contract_id to log file
        logger.info(f"save_trade() function called for contract_id: {df.iloc[row_index]['contract_id']} and timestamp: {df.iloc[row_index]['time']}")               

        # First check if the trade exists
        trade_exists = check_if_trade_exists(df.iloc[row_index]['contract_id'], df.iloc[row_index]['time'])
        
        # If the trade exists then we update the information.  Otherwise, we add a new trade
        if trade_exists:
            #update_trade(df.iloc[row_index])
            pass
        else:
            insert_trade(df.iloc[row_index])                    



"""

    CRUD Operations for the Collection table

"""
def get_all_collections():
    """
    This function returns a list of all the collections

    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.collection  
    """  
    try:  
        df = pd.read_sql_query(sql_query, con = engine)    
        return df
    except Exception as ex: 
        logger.debug(sql_query)   
        logger.error(ex)  


def get_collection(contract_id):
    """
    This function returns information for a specific collection   

    Args: contract_id - a collection's contract id
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.collection  
    WHERE contract_id = '{contract_id}'
    """   
    try:     
        df = pd.read_sql_query(sql_query, con = engine)                
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


def delete_collection(contract_id):
    """
    This function deletes a specific collection

    Args: contract_id - a collection's contract id
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.collection   
    WHERE contract_id = '{contract_id}'
    """  
    try:  
        with engine.connect() as conn:
            conn.execute(delete_query)
            print(f"{contract_id} was successfully deleted!")
    except Exception as ex:  
        logger.debug(delete_query)  
        logger.error(ex) 


def check_if_collection_exists(contract_id):
    """
    This function calls get_collection to check if the collection already exists.  
    
    Args: contract_id - a collection's contract id
    Returns: Boolean
    """        
    df = get_collection(contract_id)
    if len(df.index) > 0:
        return True        
    return False
    

def update_collection(contract_id, df):
    """
    This function updates the collection information
    
    Args: contract_id - a collection's contract id
          df - data collection of contract data
    """
    name = scrub_str(df['name'])
    descr = scrub_str(df['description'])
    royalties_fee_basic_points = scrub_int(df["royalties_fee_basic_points"])
    royalties_receiver = scrub_str(df['royalties_receiver'])

    update_query = f"""
    UPDATE {database_schema}.collection
    SET address = '{df['address']}',
        name  = '{name}',
        description = '{descr}',        
        external_url = '{df['external_url']}',
        network_id = '{df['network_id']}',
        primary_interface = '{df['primary_interface']}',
        royalties_fee_basic_points = {royalties_fee_basic_points},        
        royalties_receiver = '{royalties_receiver}',
        num_tokens = {df['num_tokens']},
        unique_owners = {df['unique_owners']},
        smart_floor_price = {df['smart_floor_price']}
    WHERE contract_id = '{contract_id}'
    """       
    try:
        with engine.connect() as conn:
            conn.execute(update_query)
    except Exception as ex:
        logger.debug(update_query)            
        logger.error(ex)   


def insert_collection(contract_id, df):
    """
    This function inserts a new collection
    
    Args: contract_id - a collection's contract id
          df - data collection of collections
    """   
    name = scrub_str(df['name'])
    descr = scrub_str(df['description'])
    royalties_fee_basic_points = scrub_int(df["royalties_fee_basic_points"])
    royalties_receiver = scrub_str(df['royalties_receiver'])

    insert_query = f"""
    INSERT INTO {database_schema}.collection (contract_id, address, name, description, external_url, network_id, primary_interface, royalties_fee_basic_points, royalties_receiver, num_tokens, unique_owners, smart_floor_price)
    VALUES ('{contract_id}', '{df['address']}', '{name}', '{descr}', '{df['external_url']}', '{df['network_id']}', '{df['primary_interface']}', {royalties_fee_basic_points}, '{royalties_receiver}', {df['num_tokens']}, {df['unique_owners']}, {df['smart_floor_price']})
    """             
    try:
        with engine.connect() as conn:
            conn.execute(insert_query)
    except Exception as ex:
        logger.debug(insert_query)        
        logger.error(ex)  
    

def save_collection(contract_df):
    """
    This function saves the collection data into a postgres database residing in AWS
    
    Args: df - data collection of collections
    """    
    for row_index in contract_df.index: 
        contract_id = contract_df['contract_id'][row_index]
        # Write contract_id to log file
        logger.info(f"save_collection() function called for contract_id: {contract_id}")    

        # First check if the collection exists
        collection_exists = check_if_collection_exists(contract_id)

        # If the collection exists then we update the information.  Otherwise, we add a new collection
        if collection_exists:
            update_collection(contract_id, contract_df.iloc[row_index])
        else:
            insert_collection(contract_id, contract_df.iloc[row_index])                                



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
    try:  
        df = pd.read_sql_query(sql_query, con = engine)    
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


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
    try:
        df = pd.read_sql_query(sql_query, con = engine)                
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


def delete_network(network_id):
    """
    This function deletes a specific network

    Args: network_id - the id of a specific blockchain
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.network   
    WHERE network_id = '{network_id}'
    """ 
    try:   
        with engine.connect() as conn:
            conn.execute(delete_query)
            print(f"{network_id} was successfully deleted!")
    except Exception as ex:  
        logger.debug(delete_query)  
        logger.error(ex) 


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
    try:
        with engine.connect() as conn:
            conn.execute(update_query)
    except Exception as ex:  
        logger.debug(update_query)  
        logger.error(ex) 


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
    try:  
        with engine.connect() as conn:
            conn.execute(insert_query)
    except Exception as ex:  
        logger.debug(insert_query)  
        logger.error(ex) 


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
    try: 
        df = pd.read_sql_query(sql_query, con = engine)    
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


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
    try:      
        df = pd.read_sql_query(sql_query, con = engine)                
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


def delete_api_request(api_id):
    """
    This function deletes a specific api request

    Args: api_id - the id of the api request
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.api   
    WHERE api_id = '{api_id}'
    """  
    try:  
        with engine.connect() as conn:
            conn.execute(delete_query)
            print(f"{api_id} was successfully deleted!")
    except Exception as ex:  
        logger.debug(delete_query)  
        logger.error(ex) 


def check_if_api_exists(api_id):
    """
    This function calls get_api to check if the api request already exists.  
    
    Args: api_id - the id of a specific api request
    Returns: Boolean
    """      
    try:  
        df = get_api_request(api_id)
        if len(df.index) > 0:
            return True        
        return False
    except Exception as ex:  
        logger.error(ex)     


def update_api_request(api_id, df):
    """
    This function updates the api information
    
    Args: api_id - the id of a specific api request
          df - data collection of network data
    """
    update_query = f"""
    UPDATE {database_schema}.api
    SET name  = '{df['name']}',
        endpoint_url = '{df['endpoint_url']}'
    WHERE api_id = '{api_id}'
    """    
    try:
        with engine.connect() as conn:
            conn.execute(update_query)
    except Exception as ex:  
        logger.debug(update_query)  
        logger.error(ex) 


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
    try:  
        with engine.connect() as conn:
            conn.execute(insert_query)
    except Exception as ex:  
        logger.debug(insert_query)  
        logger.error(ex)     


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
    try:
        df = pd.read_sql_query(sql_query, con = engine)    
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


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
    try:      
        df = pd.read_sql_query(sql_query, con = engine)                
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


def delete_whale(wallet_id):
    """
    This function deletes a specific whale

    Args: wallet_id - a whale's wallet address
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.whale  
    WHERE wallet_id = '{wallet_id}'
    """ 
    try:   
        with engine.connect() as conn:
            conn.execute(delete_query)
            print(f"{wallet_id} was successfully deleted!")
    except Exception as ex:  
        logger.debug(delete_query)  
        logger.error(ex) 


def check_if_whale_exists(wallet_id):
    """
    This function calls get_whale to check if the whale already exists.  
    
    Args: wallet_id - a whale's wallet address
    Returns: Boolean
    """ 
    try:       
        df = get_whale(wallet_id)
        if len(df.index) > 0:
            return True        
        return False
    except Exception as ex:  
        logger.error(ex)     


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
    try:
        with engine.connect() as conn:
            conn.execute(update_query)
    except Exception as ex:  
        logger.debug(update_query)  
        logger.error(ex) 


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
    try:   
        with engine.connect() as conn:
            conn.execute(insert_query)
    except Exception as ex:  
        logger.debug(insert_query)  
        logger.error(ex)     


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
    try:
        df = pd.read_sql_query(sql_query, con = engine)    
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


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
    try:    
        df = pd.read_sql_query(sql_query, con = engine)                
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


def delete_contract_maps(contract_id):
    """
    This function deletes a specific contract mapping

    Args: contract_id - a collection's contract id
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.contract_map  
    WHERE contract_id = '{contract_id}'
    """    
    try:
        with engine.connect() as conn:
            conn.execute(delete_query)
            print(f"{contract_id} was successfully deleted!")
    except Exception as ex:  
        logger.debug(delete_query)  
        logger.error(ex) 


def check_if_contract_map_exists(contract_id):
    """
    This function calls get_contract_maps to check if the contract mapping already exists.  
    
    Args: contract_id - a collection's contract id
    Returns: Boolean
    """    
    try:    
        df = get_contract_maps(contract_id)
        if len(df.index) > 0:
            return True        
        return False
    except Exception as ex:  
        logger.error(ex)     


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
    try:  
        with engine.connect() as conn:
            conn.execute(update_query)
    except Exception as ex:  
        logger.debug(update_query)  
        logger.error(ex) 


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
    try: 
        with engine.connect() as conn:
            conn.execute(insert_query)
    except Exception as ex:  
        logger.debug(insert_query)  
        logger.error(ex)     


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
    try:
        df = pd.read_sql_query(sql_query, con = engine)    
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


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
    try:
        df = pd.read_sql_query(sql_query, con = engine)                
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


def delete_token(token_id):
    """
    This function deletes a specific token

    Args: token_id - a token thats part of a contract i.e. Collection
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.token  
    WHERE token_id = '{token_id}'
    """ 
    try:   
        with engine.connect() as conn:
            conn.execute(delete_query)
            logger.info(f"{token_id} was successfully deleted!")
    except Exception as ex:  
        logger.debug(delete_query)  
        logger.error(ex) 


def check_if_token_exists(token_id):
    """
    This function calls get_token to check if the token already exists.  
    
    Args: token_id - a token thats part of a contract i.e. Collection
    Returns: Boolean
    """  
    try:      
        df = get_token(token_id)
        if len(df.index) > 0:
            return True        
        return False
    except Exception as ex:  
        logger.error(ex) 


def update_token(token_id, df):
    """
    This function updates the token information
    
    Args: token_id - a token thats part of a contract i.e. Collection
          df - data collection of token data
    """
    name = scrub_str(df['name'])
    descr = scrub_str(df['description'])
    
    update_query = f"""
    UPDATE {database_schema}.token
    SET id_num = '{df['id_num']}',
        name  = '{name}',
        description = '{descr}',
        contract_id = '{df['contract_id']}'
    WHERE token_id = '{token_id}'
    """ 
    try:   
        with engine.connect() as conn:
            conn.execute(update_query)
    except Exception as ex:  
        logger.debug(update_query)  
        logger.error(ex) 


def insert_token(token_id, df):
    """
    This function inserts a new token
    
    Args: token_id - a token thats part of a contract i.e. Collection
          df - data collection of trades
    """ 
    name = scrub_str(df['name'])
    descr = scrub_str(df['description'])
           
    insert_query = f"""
    INSERT INTO {database_schema}.token (token_id, id_num, name, description, contract_id)
    VALUES ('{token_id}', '{df['id_num']}', '{name}', '{descr}', '{df['contract_id']}')
    """    
    try:
        with engine.connect() as conn:
            conn.execute(insert_query)
    except Exception as ex:  
        logger.debug(insert_query)  
        logger.error(ex)     


def save_token(token_df):
    """
    This function saves the token data into a postgres database residing in AWS
    
    Args: df - data collection of tokens thats part of a specific contract i.e. Collection
    """    
    for row_index in token_df.index: 
        token_id = token_df['token_id'][row_index]

        # Write token_id to log file
        logger.info(f"save_token() function called for token_id: {token_id}")               

        # First check if the token exists
        token_exists = check_if_token_exists(token_id)
        
        # If the token exists then we update the information.  Otherwise, we add a new token
        if token_exists:
            #update_token(token_id, token_df.iloc[row_index])
            pass
        else:
            insert_token(token_id, token_df.iloc[row_index])                                



"""

    CRUD Operations for the Token Attribute table

"""
def get_all_token_attributes():
    """
    This function returns a list of all the token attributes

    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.token_attribute  
    """    
    try:
        df = pd.read_sql_query(sql_query, con = engine)    
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


def get_token_attribute(token_id, trait_type):
    """
    This function returns the token attribute information for a specific token

    Args: token_id - a token thats part of a contract i.e. Collection
          trait_type - part of a token's trait
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.token_attribute 
    WHERE token_id = '{token_id}'
    AND trait_type = '{trait_type}'
    """        
    try:
        df = pd.read_sql_query(sql_query, con = engine)                
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


def delete_token_attribute(token_id, trait_type):
    """
    This function deletes a specific token attribute

    Args: token_id - a token thats part of a contract i.e. Collection
          trait_type - part of a token's trait
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.token_attribute 
    WHERE token_id = '{token_id}'
    AND trait_type = '{trait_type}'
    """ 
    try:   
        with engine.connect() as conn:
            conn.execute(delete_query)
            logger.info(f"{token_id} attributes was successfully deleted!")
    except Exception as ex:  
        logger.debug(delete_query)  
        logger.error(ex) 


def check_if_token_attribute_exists(token_id, trait_type):
    """
    This function calls get_token_attributes to check if the token attribute already exists.  
    
    Args: token_id - a token thats part of a contract i.e. Collection
          trait_type - the trait for the token
    Returns: Boolean
    """  
    try:      
        df = get_token_attribute(token_id, trait_type)
        if len(df.index) > 0:
            return True        
        return False
    except Exception as ex:  
        logger.error(ex) 


def update_token_attribute(token_id, trait_type, df):
    """
    This function updates the token attribute information
    
    Args: token_id - a token thats part of a contract i.e. Collection
          df - data collection of token data
    """    
    value = scrub_str(df['value'])

    update_query = f"""
    UPDATE {database_schema}.token_attribute
    SET overall_with_trait_value = {df['overall_with_trait_value']},
        rarity_percentage  = {df['rarity_percentage']},
        value = '{value}'      
    WHERE token_id = '{token_id}'
    AND trait_type = '{trait_type}'
    """   
    try:
        with engine.connect() as conn:
            conn.execute(update_query)
    except Exception as ex:  
        logger.debug(update_query)  
        logger.error(ex) 


def insert_token_attribute(token_id, trait_type, df):
    """
    This function inserts a new token attribute
    
    Args: token_id - a token thats part of a contract i.e. Collection
          trait_type - part of a token's trait
          df - data collection of token attributes
    """ 
    value = scrub_str(df['value'])
            
    insert_query = f"""
    INSERT INTO {database_schema}.token_attribute (token_id, overall_with_trait_value, rarity_percentage, trait_type, value)
    VALUES ('{token_id}', {df['overall_with_trait_value']}, {df['rarity_percentage']}, '{trait_type}', '{value}')
    """  
    try:          
        with engine.connect() as conn:
            conn.execute(insert_query)
    except Exception as ex:  
        logger.debug(insert_query)  
        logger.error(ex)   


def save_token_attributes(token_attributes_df):
    """
    This function saves the token attributes data into a postgres database residing in AWS
    
    Args: df - data collection of tokens thats part of a specific contract i.e. Collection
    """    
    for row_index in token_attributes_df.index: 
        token_id = token_attributes_df['token_id'][row_index]
        trait_type = token_attributes_df['trait_type'][row_index]
        trait_type = scrub_str(trait_type)    

        # Write token_id and trait_type to log file
        logger.info(f"save_token_attributes() function called for token_id: {token_id} and trait_type: {trait_type}") 

        # First check if the token attribute exists
        token_attribute_exists = check_if_token_attribute_exists(token_id, trait_type)
        
        # If the token attribute exists then we update the information.  Otherwise, we add a new token attribute
        if token_attribute_exists:
            #update_token_attribute(token_id, trait_type, token_attributes_df.iloc[row_index])
            pass
        else:
            insert_token_attribute(token_id, trait_type, token_attributes_df.iloc[row_index])                                



"""

    CRUD Operations for the Social Media table

"""
def get_all_social_media():
    """
    This function returns a list of all the data in the social media table

    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.social_media 
    """   
    try: 
        df = pd.read_sql_query(sql_query, con = engine)    
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


def get_social_media(contract_id):
    """
    This function returns all social media information for a specific contract

    Args: contract_id - the contract id of the collection
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.social_media  
    WHERE contract_id = '{contract_id}'
    """  
    try:      
        df = pd.read_sql_query(sql_query, con = engine)                
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex) 


def delete_social_media(contract_id):
    """
    This function deletes all the social media for specific contract 

    Args: contract_id - the contract id of the collection
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.social_media   
    WHERE contract_id = '{contract_id}'
    """  
    try:  
        with engine.connect() as conn:
            conn.execute(delete_query)
            print(f"{contract_id} was successfully deleted!")
    except Exception as ex:  
        logger.debug(delete_query)  
        logger.error(ex) 


def check_if_social_media_exists(contract_id):
    """
    This function calls get_social_media to check if the contract social media data already exists.  
    
    Args: contract_id - the contract id of the collection
    Returns: Boolean
    """      
    try:  
        df = get_social_media(contract_id)
        if len(df.index) > 0:
            return True        
        return False
    except Exception as ex:  
        logger.error(ex)     


def update_social_media(contract_id, df):
    """
    This function updates the social media information
    
    Args: contract_id - the contract id of the collection
          df - data collection of social media data
    """
    update_query = f"""
    UPDATE {database_schema}.social_media
    SET name  = '{df['name']}',
        handle = '{df['handle']}',
        handle_url = '{df['handle_url']}',
        latest_post = '{df['latest_post']}',
        hash_tag = '{df['hash_tag']}'
    WHERE contract_id = '{contract_id}'
    """    
    try:
        with engine.connect() as conn:
            conn.execute(update_query)
    except Exception as ex:  
        logger.debug(update_query)  
        logger.error(ex) 


def insert_social_media(contract_id, df):
    """
    This function inserts a new social media account
    
    Args: contract_id - the contract id of the collection
          df - data collection of social media accounts
    """    
    insert_query = f"""
    INSERT INTO {database_schema}.social_media (contract_id, name, handle, handle_url, latest_post, hash_tag)
    VALUES ('{contract_id}', '{df['name']}', '{df['handle']}', '{df['handle_url']}', '{df['latest_post']}', '{df['hash_tag']}')
    """  
    try:  
        with engine.connect() as conn:
            conn.execute(insert_query)
    except Exception as ex:  
        logger.debug(insert_query)  
        logger.error(ex)     


def save_social_media(contract_id, df):
    """
    This function saves the social media data into a postgres database residing in AWS
    
    Args: contract_id - the contract id of the collection
          df - data collection of social media accounts
    """    
    for row_index in df.index: 
        # First check if the social media account exists
        social_media_exists = check_if_social_media_exists(contract_id)
        
        # If the social media account exists then we update the information.  Otherwise, we add a new social media account
        if social_media_exists:
            update_social_media(contract_id, df.iloc[row_index])
        else:
            insert_social_media(contract_id, df.iloc[row_index])                                




"""

    CRUD Operations for the Data_Analysis table

"""
def get_all_data_analysis(contract_id):
    """
    This function retrieves all the data analysis data done for a specific contract  

    Args: contract_id - collection's contract id
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.data_analysis  
    WHERE contract_id = '{contract_id}'
    """    
    try:
        df = pd.read_sql_query(sql_query, con = engine)    
        return df
    except Exception as ex:  
        logger.debug(sql_query)  
        logger.error(ex)      


def get_data_analysis(contract_id, time):
    """
    This function retrieves a specific data analysis information
    Check by both contract_id and time

    Args: contract_id - a collection's contract id
          time - the timestamp when the trade was made
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.data_analysis  
    WHERE contract_id = '{contract_id}'
    AND timestamp = '{time}'
    """
    try:        
        df = pd.read_sql_query(sql_query, con = engine)                
        return df
    except Exception as ex: 
        logger.debug(sql_query)   
        logger.error(ex)  


def delete_data_analysis(contract_id, time):
    """
    This function deletes a specific data analysis information 
    Check by both contract_id and time

    Args: contract_id - a collection's contract id
          time - the timestamp when the trade was made
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.data_analysis  
    WHERE contract_id = '{contract_id}'
    AND timestamp = '{time}'
    """  
    try:  
        with engine.connect() as conn:
            conn.execute(delete_query)
            print(f"The data analysis for {contract_id} at {time} was successfully deleted!")
    except Exception as ex:   
        logger.debug(delete_query) 
        logger.error(ex)  


def check_if_data_analysis_exists(contract_id, time):
    """
    This function calls get_data_analysis to check if the data analysis already exists.  
    
    Args: contract_id - a collection's contract id
          time - the timestamp when the trade was made
    Returns: Boolean
    """  
    try:      
        df = get_data_analysis(contract_id, time)
        if len(df.index) > 0:
            return True        
        return False
    except Exception as ex:    
        logger.error(ex)      


def update_data_analysis(df):
    """
    This function updates the data analysis table
    
    Args: df - data collection of data analysis information
    """
    update_query = f"""
    UPDATE {database_schema}.data_analysis
    SET percent_chg  = {round(df['percent_chg'], 2)},
        avg_percent_chg  = {round(df['avg_percent_chg'], 2)},
        standard_dev = {round(df['standard_dev'], 2)},
        avg_standard_dev = {round(df['avg_standard_dev'], 2)},
        variance = {round(df['variance'], 2)},
        co_variance = {round(df['co_variance'], 2)},
        beta = {round(df['beta'], 2)},
        whale_ratio = {round(df['whale_ratio'], 2)}
    WHERE contract_id = '{df['contract_id']}'
    AND timestamp = '{df['time']}'
    """
    try:    
        with engine.connect() as conn:
            conn.execute(update_query)
    except Exception as ex: 
        logger.debug(update_query)   
        logger.error(ex)              


def insert_data_analysis(df):
    """
    This function inserts a new data analysis done per contract
    
    Args: df - data collection of data analysis information
    """    
    insert_query = f"""
    INSERT INTO {database_schema}.data_analysis (contract_id, timestamp, percent_chg, avg_percent_chg, standard_dev, avg_standard_dev, variance, co_variance, beta, whale_ratio)
    VALUES ('{df['contract_id']}', '{df['time']}', {round(df['percent_chg'], 2)}, {round(df['avg_percent_chg'], 2)}, {round(df['standard_dev'], 2)}, {round(df['avg_standard_dev'], 2)}, {round(df['variance'], 2)}, {round(df['co_variance'], 2)}, {round(df['beta'], 2)}, {round(df['whale_ratio'], 2)})
    """  
    try:  
        with engine.connect() as conn:
            conn.execute(insert_query)
    except Exception as ex:  
        logger.debug(insert_query)  
        logger.error(ex)      


def save_data_analysis(df):
    """
    This function saves the data analysis information into a postgres database residing in AWS
    
    Args: df - data collection of data analysis information
    """    
    for row_index in df.index: 
        # First check if the data analysis information exists
        data_analysis_exists = check_if_data_analysis_exists(df.iloc[row_index]['contract_id'], df.iloc[row_index]['time'])
        
        # If the data analysis information exists then we update the information.  Otherwise, we add a new data analysis record
        if data_analysis_exists:
            update_data_analysis(df.iloc[row_index])
        else:
            insert_data_analysis(df.iloc[row_index])                    
