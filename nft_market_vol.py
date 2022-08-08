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

display(type(database_connection_string))
display(type(database_schema))

# create the database engine
engine = sqlalchemy.create_engine(database_connection_string)
inspector = inspect(engine)
inspector.get_table_names()

# Create the query
nft_market_index = """
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

# Create a Pandas DataFrame
nft_market_index_df = pd.read_sql_query(nft_market_index, con=engine)

# Filter the DataFrame beginning January 2021 - Market activity prior to this date was insignificant when compared to data from early 2021 to present
nft_market_index_df = nft_market_index_df[nft_market_index_df['year_day_month'] > '2020-12-31']

# create two separate Pandas series for number of trades and volume traded in ETH
nft_market_num_trades = nft_market_index_df.groupby('year_day_month')['total_num_trades'].sum()
nft_market_vol = nft_market_index_df.groupby('year_day_month')['total_volume'].sum()

# Ensure both series are sorted by date
nft_market_num_trades = nft_market_num_trades.sort_index()
nft_market_vol = nft_market_vol.sort_index()

# Create the Number of Trades Visual
num_trades_visual = nft_market_num_trades.hvplot(
    rot=90, 
    height=700, 
    width=1000, 
    xlabel = "Collection Name", 
    title = "NFT Market - Trades Since January 2021",
    yformatter='%.2f',
    color='blue',
)

# Display the visual
num_trades_visual

# Create the Volume in ETH Visual
vol_eth_visual = nft_market_vol.hvplot(
    rot=90, 
    height=700, 
    width=1000, 
    # ylabel = "Volume in ETH", 
    xlabel = "Collection Name", 
    title = "NFT Market - Volume in ETH Since January 2021",
    yformatter='%.2f',
    color = 'green',
)

# Display the visual
vol_eth_visual

# Overlay the two visuals
overlay = num_trades_visual * vol_eth_visual
overlay.opts(
    title = 'NFT Market - Number of Trades -Vs- Total Volume Since January 2021',
    xlabel = 'Date'
)