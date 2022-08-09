# Import the required libraries
import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv

# Libraries needed for streamlit and integrating plotting with plost
import streamlit as st
import plost

# Set the streamlit page layout to wide
st.set_page_config(layout='wide')

# Load the local env file
load_dotenv()

# Setup the database connection (use your own .env to setup the connection)
database_connection_string = os.getenv("DATABASE_URL")
database_schema = os.getenv("DATABASE_SCHEMA")

# Create the database engine
engine = sqlalchemy.create_engine(database_connection_string)

# Create the query
os_top_collection_index = """
SELECT c.contract_id,
       c.name,
       c.description,
       MAX(avg_price) as highest_avg_price_ever_reached,
       MAX(min_price) as highest_minimum_price_ever_reached,
       MAX(max_price) as highest_max_price_ever_reached,
       SUM(t.volume) as total_volume
FROM collection c
INNER JOIN trade t ON t.contract_id = c.contract_id
INNER JOIN network n ON n.network_id = c.network_id
WHERE n.network_id = 'ethereum' 
AND c.name NOT IN ('','New 0x495f947276749Ce646f68AC8c248420045cb7b5eLock', 'pieceofshit')
GROUP BY c.contract_id, c.name
HAVING MAX(avg_price) > 0
ORDER BY SUM(t.volume) DESC
"""

# Convert the query to a Pandas DataFrame
os_top_collection_index_df = pd.read_sql_query(os_top_collection_index, con=engine)

# filter the query for only the top ten collections listed on OpenSea
os_top_collection_index_df = os_top_collection_index_df[os_top_collection_index_df['name'].str.contains(
    'CryptoPunks|BoredApeYachtClub|MutantApeYachtClub|Otherdeed|Azuki|CloneX|Moonbirds|Doodles|Cool Cats|BoredApeKennelClub')==True]

# Drop redundant collections
os_top_collection_index_df = os_top_collection_index_df.drop(index=52)
os_top_collection_index_df = os_top_collection_index_df.drop(index=62)
os_top_collection_index_df = os_top_collection_index_df.drop(index=72)

# Select only the required columns
os_top_collection_index_df = os_top_collection_index_df[['name', 'total_volume']]

# Rename the columns
os_top_collection_index_df.columns = ['Collection Name', 'Volume in ETH']

# Sort the DataFrame by Collection Name
os_top_collection_index_df = os_top_collection_index_df.sort_values('Collection Name')

# Row 2
e1, e2 = st.columns(2)
with e1:
    st.markdown('### Open Sea Top Ten Collections Volume in ETH - All Time')
    plost.bar_chart(
        data = os_top_collection_index_df,
        bar = 'Collection Name',
        value = 'Volume in ETH',
        color = 'green',
        width = 500,
        height = 500,
    )        
    with st.expander("See explanation"):
        st.write("""These two charts compare the total volume (in ETH) and the number of trades of Open Sea's top ten NFT collections. It is interesting to note that the all time volume for just ten NFT collections is 4,201,212ETH at today's current prices of Ether that's $7.14 Billion. Aditionally the top 4 collections by volume make up almost 70% of the total volume across the top ten collections, clearly demonstrating the extreme difference in value of the top 4 collections -vs- the other six. Three of the top four collections also were released by the same creators (Yuga Labs, demonstrating how concentrated and young the NFT market really is.""")

# Create the 2nd query
os_top_collection_index_2 = """
SELECT t.contract_id,
       c.name,
       DATE_TRUNC('day', t.timestamp) as year_day_month,
       SUM(t.volume) as total_volume,
       SUM(t.num_trades) as total_num_trades,
       SUM(t.unique_buyers) as total_unique_buyers
FROM trade t
INNER JOIN collection c ON c.contract_id = t.contract_id
INNER JOIN network n ON n.network_id = c.network_id
WHERE n.network_id = 'ethereum' 
AND c.name NOT IN ('','New 0x495f947276749Ce646f68AC8c248420045cb7b5eLock', 'pieceofshit', 'Uniswap V3 Positions NFT-V1','More Loot',
                  'NFTfi Promissory Note','dementorstownwtf','ShitBeast','mcgoblintownwtf','LonelyPop','Pablos','For the Culture','Hype Pass', 'Moonbirds Oddities', 'AIMoonbirds', 'Bound NFT CloneX')
GROUP BY t.contract_id, c.name, DATE_TRUNC('day', t.timestamp)
HAVING MAX(avg_price) > 0
ORDER BY DATE_TRUNC('day', t.timestamp)  ASC
"""

# Convert the query to a Pandas DataFrame
os_top_collection_index_2 = pd.read_sql_query(os_top_collection_index_2, con=engine)

# filter the query for only the top ten collections listed on OpenSea"
os_top_collection_index_2 = os_top_collection_index_2[os_top_collection_index_2['name'].str.contains('CryptoPunks|BoredApeYachtClub|MutantApeYachtClub|Otherdeed|Azuki|CloneX|Moonbirds|Doodles|Cool Cats|BoredApeKennelClub')==True]

# Select only the required columns
os_top_collection_index_2 = os_top_collection_index_2[['name', 'total_num_trades']]

# Rename the columns
os_top_collection_index_2.columns = ['Collection Name', 'Number of Trades']

# Group the data by Collection Name and sum the number of trades
os_top_collection_index_num_trades = os_top_collection_index_2.groupby('Collection Name')['Number of Trades'].sum()

# Sort the data in alphabetical order to match the other chart
os_top_collection_index_num_trades = os_top_collection_index_num_trades.sort_index()

# Convert the series to a Pandas DataFrame for Streamlit Integration
os_top_collection_index_num_trades = os_top_collection_index_num_trades.to_frame()

# Reset the index for Streamlit Integration
os_top_collection_index_num_trades.reset_index(inplace=True)

# Row D
with e2:
    st.markdown('### Open Sea Top Ten Collections Number of Trades - All Time')
    plost.bar_chart(
        data = os_top_collection_index_num_trades,
        bar = 'Collection Name',
        value = 'Number of Trades',
        color = 'blue',
        width = 500,
        height = 500,
    )
    with st.expander("See explanation"):
        st.write("""These two charts compare the total volume (in ETH) and the number of trades of Open Sea's top ten NFT collections. It is interesting to note that the all time volume for just ten NFT collections is 4,201,212ETH at today's current prices of Ether that's $7.14 Billion. Aditionally the top 4 collections by volume make up almost 70% of the total volume across the top ten collections, clearly demonstrating the extreme difference in value of the top 4 collections -vs- the other six. Three of the top four collections also were released by the same creators (Yuga Labs, demonstrating how concentrated and young the NFT market really is.""")