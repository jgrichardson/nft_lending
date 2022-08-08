# Import the required libraries
import os
import requests
import json
import pandas as pd
import hvplot.pandas
import sqlalchemy
from sqlalchemy import inspect
from dotenv import load_dotenv
import numpy as np
import holoviews as hv
hv.extension('bokeh')

# Libraries needed for streamlit and integrating plotting with plost
import streamlit as st
import plost

# Support bokeh extension
hv.extension('bokeh', logo=False)

# Set the streamlit page layout to wide
st.set_page_config(layout='wide')

# Load the local env file
load_dotenv()

# Setup the database connection (use your own .env to setup the connection)
database_connection_string = os.getenv("DATABASE_URL")
database_schema = os.getenv("DATABASE_SCHEMA")

# Create the database engine
engine = sqlalchemy.create_engine(database_connection_string)
inspector = inspect(engine)
inspector.get_table_names()

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

# filter the query for only the top ten collections listed on OpenSea"
os_top_collection_index_df = os_top_collection_index_df[os_top_collection_index_df['name'].str.contains(
    'CryptoPunks|BoredApeYachtClub|MutantApeYachtClub|Otherdeed|Azuki|CloneX|Moonbirds|Doodles|Cool Cats|BoredApeKennelClub')==True]

# Drop redundant collections
os_top_collection_index_df = os_top_collection_index_df.drop(index=52)
os_top_collection_index_df = os_top_collection_index_df.drop(index=62)
os_top_collection_index_df = os_top_collection_index_df.drop(index=72)

# Sort the DataFrame by Collection Name
os_top_collection_index_df = os_top_collection_index_df.sort_values('name')

# Visualize OpenSea's top collections by all time volume
side_1 = os_top_collection_index_df.hvplot.bar(
    x='name', 
    y='total_volume', 
    rot=90, 
    height=700, 
    width=1000, 
    ylabel = "Volume in ETH", 
    xlabel = "Collection Name", 
    title = "OpenSea Top Ten Collections by Volume All Time",
    yformatter='%.2f'
)

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
os_top_collection_index_2 = os_top_collection_index_2[['name', 'total_volume', 'total_num_trades', 'total_unique_buyers']]

# Rename the columns
os_top_collection_index_2.columns = ['name', 'volume', 'num_trades', 'unique_buyers']

# Group the data by Collection Name and sum the number of trades
os_top_collection_index_num_trades = os_top_collection_index_2.groupby('name')['num_trades'].sum()

# Sort the data in alphabetical order to match the other chart
os_top_collection_index_num_trades = os_top_collection_index_num_trades.sort_index()

# Visualize Open Sea's top collections by number of trades
side_2 = os_top_collection_index_num_trades.hvplot.bar(
    title = 'Open Sea Top Collections Number of Trades January 2021 - Present',
    rot = 90, 
    height = 700, 
    width = 1000,  
    xlabel = "Collection Name",
    ylabel = "Number of Trades",
    yformatter='%.2f',
    color = 'green'
)

# Combine the two Visuals
side_by_side = side_1 + side_2

# Row D
d1, d2, d3, d4 = st.columns(4)
with d3:
    st.markdown('### Open Sea Top Ten Collections by Volume and Number of Trades')
    st.bokeh_chart(hv.render(side_by_side, backend='bokeh'))
    with st.expander("See explanation"):
        st.write("""These two charts compare the total volume (in ETH) and the number of trades of Open Sea's top ten NFT collections. It is interesting to note that the all time volume for just ten NFT collections is 4,201,212ETH at today's current prices of Ether that's $7.14 Billion. Aditionally the top 4 collections by volume make up almost 70% of the total volume across the top ten collections, clearly demonstrating the extreme difference in value of the top 4 collections -vs- the other six. Three of the top four collections also were released by the same creators (Yuga Labs, demonstrating how concentrated and young the NFT market really is.""")