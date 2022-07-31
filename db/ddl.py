# Import Libraries
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import inspect
import logging


# Get Logger
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
        DROP TABLE IF EXISTS Contract;
        """,
        """
        DROP TABLE IF EXISTS Contract_Map;
        """,
        """
        DROP TABLE IF EXISTS Whale;
        """,
        """
        DROP TABLE IF EXISTS Api;
        """,
        """
        DROP TABLE IF EXISTS Trade;
        """
    ]
    try:
        with engine.connect() as conn:
            # Drop tables one by one
            for tbl in drop_tbls:
                conn.execute(tbl)
                print(tbl + " Successfully Dropped!")
    except Exception as ex:
        logger.exception(ex)


def create_tables():
    """ create tables in the database"""
    create_tbls = [
        """
        CREATE TABLE Network(
            network_id VARCHAR(50) PRIMARY KEY,
            short_name VARCHAR(50)
        )
        """,
        """
        CREATE TABLE Contract(
            contract_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(100),
            network_id VARCHAR(50)
        )
        """,
        """
        CREATE TABLE Contract_Map(
            contract_id VARCHAR(50),
            new_contract_id VARCHAR(50)
        )
        """,
        """
        CREATE TABLE Whale(
            wallet_id VARCHAR(50),
            contract_id VARCHAR(50)
        )
        """,
        """
        CREATE TABLE API(
            api_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(100),
            endpoint VARCHAR(250)
        )
        """,
        """
        CREATE TABLE Trade(
            contract_id VARCHAR(50) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            avg_price NUMERIC,
            max_price NUMERIC,
            min_price NUMERIC,
            num_trades INT,
            unique_buyers INT,
            floor_price NUMERIC,
            ceiling_price NUMERIC,
            volume INT,
            period VARCHAR(10),
            api_id VARCHAR(50)
        )
        """
    ]

    try:
        with engine.connect() as conn:
            # create tables one by one
            for tbl in create_tbls:
                conn.execute(tbl)
                print(tbl + " Successfully Created!")
    except Exception as ex:
        logger.exception(ex)
    
def add_constraints():
    """ add unique constraints to tables in the database"""
    unique_constraints = [
        """
        ALTER TABLE Trade
        ADD CONSTRAINT contract_timestamp UNIQUE (contract_id, timestamp);
        """
    ]
    try:
        with engine.connect() as conn:
            # add unique constraints one by one
            for constraint in unique_constraints:
                conn.execute(constraint)
                print(constraint + " Successfully Added!")
    except Exception as ex:
        logger.exception(ex)

if __name__ == '__main__':
    # Display all table names in the database
    inspector = inspect(engine)
    database_tables = inspector.get_table_names(database_schema)
    print(database_tables)

    # Drop system tables
    drop_tables()

    # Create system tables
    create_tables()

    # Add unique constraints to tables
    add_constraints()

    # Display all table names in the database
    inspector = inspect(engine)
    database_tables = inspector.get_table_names(database_schema)
    print(database_tables)




