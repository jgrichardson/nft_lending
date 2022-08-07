# Import Libraries
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import inspect
import logging


# Get Logger
logging.basicConfig(filename='ddl.log', filemode='w', level=logging.INFO, format='%(levelname)s: %(asctime)s - %(message)s')
logger = logging.getLogger()

# Load .env environment variables
load_dotenv()

# Read in database settings
database_connection_string = os.getenv("DATABASE_URI")

# Read in database schema
database_schema = os.getenv("DATABASE_SCHEMA")

# Create database connection
engine = create_engine(database_connection_string)

def drop_tables():
    """ drop tables in the database"""
    drop_tbls = [
        """
        DROP TABLE IF EXISTS Network;
        """,
        """
        DROP TABLE IF EXISTS Collection;
        """,
        """
        DROP TABLE IF EXISTS Contract_Map;
        """,
        """
        DROP TABLE IF EXISTS Token;
        """,
        """
        DROP TABLE IF EXISTS Token_Attribute;
        """,
        """
        DROP TABLE IF EXISTS Whale;
        """,
        """
        DROP TABLE IF EXISTS Api;
        """,
        """
        DROP TABLE IF EXISTS Trade;
        """,
        """
        DROP TABLE IF EXISTS Social_Media;
        """,
        """
        DROP TABLE IF EXISTS data_analysis;
        """        
    ]
    try:
        with engine.connect() as conn:
            # Drop tables one by one
            for tbl in drop_tbls:
                conn.execute(tbl)
                logger.info(tbl + " Successfully Dropped!")
    except Exception as ex:
        logger.debug(drop_tbls)
        logger.exception(ex)


def create_tables():
    """ create tables in the database"""
    create_tbls = [
        """
        CREATE TABLE Network(
            network_id VARCHAR PRIMARY KEY,
            short_name VARCHAR
        )
        """,
        """
        CREATE TABLE Collection(
            contract_id VARCHAR PRIMARY KEY,
            address VARCHAR,               
            name VARCHAR,
            description VARCHAR,         
            external_url VARCHAR,            
            network_id VARCHAR,
            primary_interface VARCHAR,
            royalties_fee_basic_points INT,
            royalties_receiver VARCHAR,
            num_tokens INT,
            unique_owners INT,
            smart_floor_price NUMERIC
        )
        """,
        """
        CREATE TABLE Contract_Map(
            contract_id VARCHAR,
            new_contract_id VARCHAR
        )
        """,
        """
        CREATE TABLE Token(
            token_id VARCHAR,
            id_num   VARCHAR,
            name VARCHAR,
            description VARCHAR,
            contract_id VARCHAR,
            rarity_score NUMERIC,
            ranking INT
        )
        """,
        """
        CREATE TABLE Token_Attribute(
            token_id VARCHAR,
            overall_with_trait_value INT,
            rarity_percentage NUMERIC,
            trait_type VARCHAR,
            value VARCHAR
        )
        """,
        """
        CREATE TABLE Whale(
            wallet_id VARCHAR,
            contract_id VARCHAR
        )
        """,
        """
        CREATE TABLE API(
            api_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            endpoint_url VARCHAR
        )
        """,
        """
        CREATE TABLE Trade(
            contract_id VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            avg_price NUMERIC,
            max_price NUMERIC,
            min_price NUMERIC,
            num_trades INT,
            unique_buyers INT,
            volume INT,
            period VARCHAR,
            type VARCHAR,
            api_id VARCHAR
        )
        """,
        """
        CREATE TABLE Social_Media(
            contract_id VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            handle VARCHAR NOT NULL,
            handle_url VARCHAR,
            latest_post VARCHAR,
            hash_tag VARCHAR
        )
        """,
        """
        CREATE TABLE Data_Analysis (
            contract_id VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            percent_chg      NUMERIC,
            avg_percent_chg  NUMERIC,
            standard_dev     NUMERIC,
            avg_standard_dev NUMERIC,
            variance         NUMERIC,
            co_variance      NUMERIC,
            beta             NUMERIC,
            whale_ratio      NUMERIC
        )
        """
    ]

    try:
        with engine.connect() as conn:
            # create tables one by one
            for tbl in create_tbls:
                conn.execute(tbl)
                logger.info(tbl + " Successfully Created!")
    except Exception as ex:
        logger.debug(create_tbls)
        logger.exception(ex)
    
def add_constraints():
    """ add unique constraints to tables in the database"""
    unique_constraints = [
        """
        ALTER TABLE Contract_Map
        ADD CONSTRAINT contract_new_contract UNIQUE (contract_id, new_contract_id);
        """ ,        
        """
        ALTER TABLE Trade
        ADD CONSTRAINT contract_timestamp UNIQUE (contract_id, timestamp);
        """,
        """
        ALTER TABLE Data_Analysis
        ADD CONSTRAINT contract_timestamp_da UNIQUE (contract_id, timestamp);
        """        
    ]
    try:
        with engine.connect() as conn:
            # add unique constraints one by one
            for constraint in unique_constraints:
                conn.execute(constraint)
                logger.info(constraint + " Successfully Added!")
    except Exception as ex:
        logger.debug(unique_constraints)
        logger.exception(ex)

def add_unique_indexes():
    """ ad unique index to tables in the database"""
    unique_indexes = [
        """
        CREATE UNIQUE INDEX idx_token_collection
        ON token (token_id, contract_id)
        """,
        """
        CREATE UNIQUE INDEX idx_token_trait
        ON token_attribute (token_id, trait_type)
        """    
    ]
    try:
        with engine.connect() as conn:
            # add unique indexes one by one
            for index in unique_indexes:
                conn.execute(index)
                logger.info(index + " Successfully Added!")
    except Exception as ex:
        logger.debug(unique_indexes)
        logger.exception(ex)


if __name__ == '__main__':
    try:
        # Display all table names in the database
        inspector = inspect(engine)
        database_tables = inspector.get_table_names(database_schema)
        logger.info(database_tables)

        # Drop system tables
        drop_tables()

        # Create system tables
        create_tables()

        # Add unique constraints to tables
        add_constraints()

        # Add unique indexes to tables
        add_unique_indexes()
        
        # Display all table names in the database
        inspector = inspect(engine)
        database_tables = inspector.get_table_names(database_schema)
        logger.info(database_tables)
    except Exception as ex:
        logger.debug(database_tables)
        logger.exception(ex)



