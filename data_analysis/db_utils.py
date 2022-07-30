import pandas as pd
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
    This function returns a list of all the existing tables in the db

    Returns: List
    """
    inspector = inspect(engine)
    tables = inspector.get_table_names(database_schema)
    return tables


def get_all_trades(contract_id):
    """
    This function gets all the trades for a contract from the db    

    Args: contract_id - collection's contract id
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.trades   
    WHERE contract_id = '{contract_id}'
    """    
    df = pd.read_sql_query(sql_query, con = engine)    
    return df


def get_trade(contract_id, time):
    """
    This function gets a specific trade information regarding a trade from the db    
    Check by both contract_id and time

    Args: contract_id - a collection's contract id
          time - the timestamp when the trade was made
    Returns: DataFrame
    """       
    sql_query = f"""
    SELECT * 
    FROM {database_schema}.trades   
    WHERE contract_id = '{contract_id}'
    AND timestamp = '{time}'
    """        
    df = pd.read_sql_query(sql_query, con = engine)                
    return df


def delete_trade(contract_id, time):
    """
    This function deletes a specific trade from the db  
    Check by both contract_id and time

    Args: contract_id - a collection's contract id
          time - the timestamp when the trade was made
    """       
    delete_query = f"""
    DELETE FROM {database_schema}.trades   
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
    

def update_trade(network_id, period, contract_id, df):
    """
    This function updates the trade table in the database
    
    Args: network_id - the id of the blockchain the collection is part of
          period - the data frequency from the api request
          contract_id - a collection's contract id
          df - data collection of trades
    """
    update_query = f"""
    UPDATE {database_schema}.trades
    SET avg_price  = {round(df['avg_price'], 2)},
        max_price  = {round(df['max_price'], 2)},
        min_price  = {round(df['min_price'], 2)},
        num_trades = {df['trades']},
        unique_buyers = {df['unique_buyers']},
        volume = {round(df['volume'], 2)},
        period = '{period}'
    WHERE contract_id = '{contract_id}'
    AND timestamp = '{df['time']}'
    """    
    with engine.connect() as conn:
        conn.execute(update_query)


def insert_trade(network_id, period, contract_id, df):
    """
    This function inserts a new trade into the db
    
    Args: network_id - the id of the blockchain the collection is part of
          period - the data frequency from the api request
          contract_id - a collection's contract id
          df - data collection of trades
    """    
    insert_query = f"""
    INSERT INTO {database_schema}.trades (contract_id, timestamp, avg_price, max_price, min_price, num_trades, unique_buyers, volume, period)
    VALUES ('{contract_id}', '{df['time']}', {round(df['avg_price'], 2)}, {round(df['max_price'], 2)}, {round(df['min_price'], 2)}, {df['trades']}, {df['unique_buyers']}, {round(df['volume'], 2)}, '{period}')
    """    
    with engine.connect() as conn:
        conn.execute(insert_query)
    

def save_collection(network_id, period, contract_id, df):
    """
    This function saves the collection data into a postgres database residing in AWS
    
    Args: network_id - the id of the blockchain the collection is part of
          period - the data frequency from the api request
          contract_id - a collection's contract id
          df - data collection of trades
    """    
    for row_index in df.index: 
        # First check if the trade exists
        trade_exists = check_if_trade_exists(contract_id, df.iloc[row_index]['time'])
        
        # If the trade exists then we update the information.  Otherwise, we add a new trade
        if trade_exists:
            update_trade(network_id, period, contract_id, df.iloc[row_index])
        else:
            insert_trade(network_id, period, contract_id, df.iloc[row_index])                    