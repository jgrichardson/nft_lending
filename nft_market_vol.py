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

# create the database engine
engine = sqlalchemy.create_engine(database_connection_string)

def create_nft_market_vol():
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

    # Rename the columns
    nft_market_index_df.columns = ['contract_id', 'name', 'Date', 'Volume in ETH', 'Number of Trades', 'Unique Buyers']

    # create a Pandas series for volume traded in ETH
    nft_market_vol_df = nft_market_index_df.groupby('Date')['Volume in ETH'].sum()

    # Ensure series is sorted by date
    nft_market_vol_df = nft_market_vol_df.sort_index()

    # Convert series to DataFrame for integration with Plost
    nft_market_vol_df = nft_market_vol_df.to_frame()

    # Reset the Index for integration with Plost
    nft_market_vol_df.reset_index(inplace=True)

    return nft_market_vol_df

# Publish to Streamlit
d1, d2 = st.columns(2)
with d1:
    st.markdown('### NFT Market Volume')
    plost.line_chart(
        data = create_nft_market_vol(),
        x = 'Date',
        y = 'Volume in ETH',
        color = 'green',
        width = 500,
        height = 300,
    )        
    with st.expander("See explanation"):
        st.write("""This chart shows the total volume (in ETH) of all NFT collections traded from January 2021 to present day. You can see from January 1st, 2021 to January 31st, 2022 the NFT market grew in volume by 128,792%. The total volume of the NFT Market at its peak traded over $820 million on a single day. A line of best fit would clearly demonstrate a growing market as time progresses.""")
    