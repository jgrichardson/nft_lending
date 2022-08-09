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

# Convert the database to a Pandas DataFrame
os_top_collection_index_df = pd.read_sql_query(os_top_collection_index, con=engine)

# filter the query for only the top ten collections listed on OpenSea"
os_top_collection_index_df = os_top_collection_index_df[os_top_collection_index_df['name'].str.contains('CryptoPunks|BoredApeYachtClub|MutantApeYachtClub|Otherdeed|Azuki|CloneX|Moonbirds|Doodles|Cool Cats|BoredApeKennelClub')==True]

# Select only the required columns
os_top_collection_index_df = os_top_collection_index_df[['year_day_month', 'total_volume']]

# Rename the columns
os_top_collection_index_df.columns = ['Date', 'Volume in ETH']

# Only select data from January 2021 to present. Prior to this date the NFT market activity is insignificant for this analysis
os_top_collection_index_df = os_top_collection_index_df[os_top_collection_index_df['Date'] > '2020-12-31']

# Group the data by date and sum the volume
os_top_collection_index_volume = os_top_collection_index_df.groupby('Date')['Volume in ETH'].sum()

# Convert the series to a Pandas DataFrame for Streamlit Integration
os_top_collection_index_volume = os_top_collection_index_volume.to_frame()

# Reset the index for Streamlit Integration
os_top_collection_index_volume.reset_index(inplace=True)

# Row D
D1, d2 = st.columns(2)
with d2:
    st.markdown("### Open Sea's Top Ten Collections Volume Traded From January 2021 to Present")
    plost.line_chart(
        data = os_top_collection_index_volume,
        x = 'Date',
        y = 'Volume in ETH',
        color = 'green',
        width = 500,
        height = 300,
    )
    with st.expander("See explanation"):
        st.write("""This chart shows the total volume (in ETH) of Open Sea's Top Ten Collections traded over time. It is clear from the data that the top ten collections continue to trade at higher highs every time there is an interest spike in the NFT market.""") 