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
nft_market_vol = nft_market_index_df.groupby('year_day_month')['total_volume'].sum()

# Ensure series is sorted by date
nft_market_vol = nft_market_vol.sort_index()

# Create the Volume in ETH Visual
vol_eth_visual = nft_market_vol.hvplot(
    rot=90, 
    height=700, 
    width=1000, 
    ylabel = "Volume in ETH", 
    xlabel = "Collection Name", 
    yformatter='%.2f',
    color = 'green',
)

# Row D
d1, d2, d3, d4 = st.columns(4)
with d1:
    st.markdown('### NFT Market Volume')
    st.bokeh_chart(hv.render(vol_eth_visual, backend='bokeh'))
    with st.expander("See explanation"):
        st.write("""This chart shows the total volume (in ETH) of all NFT collections traded from January 2021 to present day. You can see from January 1st, 2021 to January 31st, 2022 the NFT market grew in volume by 128,792%. The total volume of the NFT Market at its peak traded over $820 million on a single day. A line of best fit would clearly demonstrate a growing market as time progresses.""")
            