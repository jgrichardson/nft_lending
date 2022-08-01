# Import Libraries
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging


# Get Logger
logging.basicConfig(filename='dml.log', filemode='w', level=logging.INFO, format='%(levelname)s: %(asctime)s - %(message)s')
logger = logging.getLogger()

# Load .env environment variables
load_dotenv()

# Retrieve the database settings from .env file
database_connection_string = os.getenv("DATABASE_URI")

# Retrieve the database schema from .env file
database_schema = os.getenv("DATABASE_SCHEMA")

# Create a database connection
engine = create_engine(database_connection_string, echo = False)



def delete_network_data():
    """ Delete System Data"""
    delete_networks = [ 
        f"""
        DELETE FROM {database_schema}.network
        """            
    ]

    try:
        with engine.connect() as conn:
            # delete records one by one
            for rec in delete_networks:
                conn.execute(rec)
                logger.info(rec + " Successfully Deleted!")
    except Exception as ex:
        logger.debug(delete_networks)
        logger.exception(ex)
        


def add_network_data():
    """ Insert System Data"""
    insert_networks = [ 
        f"""
        INSERT INTO {database_schema}.network (network_id, short_name)
        VALUES ('bitcoin', 'BTC')
        """,    
        f"""
        INSERT INTO {database_schema}.network (network_id, short_name)
        VALUES ('ethereum', 'ETH')        
        """,
        f"""
        INSERT INTO {database_schema}.network (network_id, short_name)
        VALUES ('solana', 'SOL')        
        """,
        f"""
        INSERT INTO {database_schema}.network (network_id, short_name)
        VALUES ('xrp', 'XRP')        
        """,
        f"""
        INSERT INTO {database_schema}.network (network_id, short_name)
        VALUES ('polygon', 'MATIC')        
        """                         
    ]

    try:
        with engine.connect() as conn:
            # insert records one by one
            for rec in insert_networks:
                conn.execute(rec)
                logger.info(rec + " Successfully Inserted!")
    except Exception as ex:
        logger.debug(insert_networks)
        logger.exception(ex)
    


def delete_api_data():
    """ Delete System Data"""
    delete_apis = [ 
        f"""
        DELETE FROM {database_schema}.api
        """            
    ]

    try:
        with engine.connect() as conn:
            # delete records one by one
            for rec in delete_apis:
                conn.execute(rec)
                logger.info(rec + " Successfully Deleted!")
    except Exception as ex:
        logger.debug(delete_apis)
        logger.exception(ex)
        


def add_api_data():
    """ Insert System Data"""
    insert_apis = [ 
        f"""
        INSERT INTO {database_schema}.api (api_id, name, endpoint_url)
        VALUES ('rarify', 'The Rarify NFT APIs', 'https://api.rarify.tech/data/')
        """   
    ]

    try:
        with engine.connect() as conn:
            # insert records one by one
            for rec in insert_apis:
                conn.execute(rec)
                logger.info(rec + " Successfully Inserted!")
    except Exception as ex:
        logger.debug(insert_apis)
        logger.exception(ex)    



if __name__ == '__main__':
    # Delete System Data
    delete_network_data()
    delete_api_data()

    # Add System Data
    add_network_data()
    add_api_data()
   
