import pandas as pd
from dotenv import load_dotenv # For loading env variables
import os # Utility library
import sqlalchemy
import altair as alt
from pathlib import Path
import streamlit as st
import hvplot.pandas
import holoviews as hv
import plost
import seaborn as sns

load_dotenv()

# Setup the database connection (use your own .env to setup the connection)
database_connection_string = os.getenv("DATABASE_URL")
database_schema = os.getenv("DATABASE_SCHEMA")

# create the database engine
engine = sqlalchemy.create_engine(database_connection_string)

def get_sentiment_data():
    results_dict = {'tag': ['#meebits',
      '#cryptopunks',
      '#terraforms',
      '#mayc',
      '#moonbirds',
      '#azuki',
      '#bayc',
      '#dreadfulz',
      '#clonex',
      '#beanz'],
     'pos': [3, 12, 31, 8, 14, 3, 6, 6, 24, 26],
     'neg': [39, 35, 6, 10, 33, 47, 27, 21, 22, 23],
     'neu': [8, 3, 13, 32, 3, 0, 17, 0, 4, 1]}
    sentiment_df = pd.DataFrame(results_dict)
    return sentiment_df

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
    
def plot_std():
        csv_path = Path('./static_data/standard_deviations.csv')
        std_devs = pd.read_csv(csv_path)
        plost_chart = plost.bar_chart(
            data = std_devs,
            bar = 'Collections',
            value= 'std_deviations',
            title = 'NFT Collections by Standard Deviation',
            color = 'red',
            width = 500,
            height = 300,
        )
        return plost_chart

def query_correlation():
    sql_query = """
    SELECT t.contract_id,
           c.name as collection_name,
           c.address,
           tok.token_id,
           tok.id_num,
           tok.name as token_name,
           tok.rarity_score,
           tok.ranking,
           ct.average_token_rarity_score_for_collection,
           MIN(t.avg_price) as avg_price_for_collection,
           MIN(t.min_price) as min_price_for_collection,
           MAX(t.max_price) as max_price_for_collection,
           SUM(t.volume) as total_volume_for_collection,
           SUM(t.num_trades) as total_num_trades_for_collection,
           SUM(t.unique_buyers) as total_unique_buyers_for_collection
    FROM network n
    INNER JOIN collection c ON c.network_id = n.network_id
    INNER JOIN (SELECT contract_id, ROUND(AVG(rarity_score), 2) AS average_token_rarity_score_for_collection FROM token GROUP BY contract_id) ct ON ct.contract_id = c.contract_id
    INNER JOIN token tok ON tok.contract_id = ct.contract_id
    INNER JOIN trade t ON t.contract_id = c.contract_id
    WHERE n.network_id = 'ethereum' 
    AND c.name IN ('CryptoPunks', 'BoredApeYachtClub', 'MutantApeYachtClub', 'Otherdeed', 'Azuki', 'CloneX', 'Moonbirds', 'Doodles', 'Meebits', 'Cool Cats', 'BoredApeKennelClub')
    AND tok.ranking = 1
    GROUP BY t.contract_id, c.name, c.address, tok.token_id, tok.id_num, tok.name, tok.rarity_score, tok.ranking, ct.average_token_rarity_score_for_collection
    HAVING MIN(t.avg_price) > 0.0
    ORDER BY SUM(t.volume)  DESC
    """
    df = pd.read_sql_query(sql_query, con = engine)
    return df


def plot_collection_max_price(df):
    collection_max_price_df = df.drop(columns=['contract_id', 'address', 'token_id', 'id_num', 'rarity_score', 'token_name', 'ranking', 'average_token_rarity_score_for_collection', 'avg_price_for_collection', 'min_price_for_collection', 'total_volume_for_collection', 'total_num_trades_for_collection', 'total_unique_buyers_for_collection'])
    return collection_max_price_df

def plot_rarity_score(df):
    collection_rarity_score_df = df.drop(columns=['contract_id', 'address', 'token_id', 'id_num', 'token_name', 'ranking', 'average_token_rarity_score_for_collection', 'max_price_for_collection', 'avg_price_for_collection', 'min_price_for_collection', 'total_volume_for_collection', 'total_num_trades_for_collection', 'total_unique_buyers_for_collection'])
    return collection_rarity_score_df


def plot_std_index():
    csv_path = Path('./static_data/std_devs_top_collections_index.csv')
    std_devs = pd.read_csv(csv_path, index_col='time', infer_datetime_format=True, parse_dates=True)
    plot = std_devs.hvplot(ylabel="Standard Deviation", title="NFT Market Standard Deviations over Time").opts(xrotation=90)
    return st.bokeh_chart(hv.render(plot, backend='bokeh'))

def plot_unstoppable_domains():
    csv_path = Path('./static_data/unstoppable_domains.csv')
    unstoppable_domains_df = pd.read_csv(csv_path, index_col='time', parse_dates=True, infer_datetime_format=True)
    plot = unstoppable_domains_df['avg_price'].hvplot(ylabel="Average Price", title="Unstoppable Domains Price Action over time", height=511).opts(xrotation=90)
    return st.bokeh_chart(hv.render(plot, backend='bokeh'))

def plot_betas():
    csv_path = Path('./static_data/betas.csv')
    beta_values = pd.read_csv(csv_path)
    plost_chart = plost.bar_chart(
        data = beta_values,
        bar = 'Collections',
        value= 'Betas',
        title = 'NFT Market Betas For Top Collections',
        color = 'blue',
        width = 500,
        height = 500,
    ) 
    return beta_values


def plot_index():
    """
    Returns a sns heatmap object.
    Needs to be compiled into a streamlit object.

    """
    csv_path = Path('./static_data/top_collections_data.csv')
    index = pd.read_csv(csv_path)
    keys = ['avg_price', 'min_price', 'max_price', 'volume', 'pct_chg', 'std_dev']
    index_correlation = index[keys].corr()
    return sns.heatmap(index_correlation)

def plot_mc_sim():
    csv_path = Path('./static_data/mc_cum_return.csv')
    cum_returns = pd.read_csv(csv_path)  
    return cum_returns

def mc_sim_describe():
    csv_path = Path('./static_data/mc_cum_return.csv')
    cum_returns = pd.read_csv(csv_path)
    return cum_returns.describe()

def create_os_collection_index():
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
    os_top_collection_index_vol_df = os_top_collection_index_df.groupby('Date')['Volume in ETH'].sum()

    # Convert the series to a Pandas DataFrame for Streamlit Integration
    os_top_collection_index_vol_df = os_top_collection_index_vol_df.to_frame()

    # Reset the index for Streamlit Integration
    os_top_collection_index_vol_df.reset_index(inplace=True)

    return os_top_collection_index_vol_df

def create_top_collections_one():
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
    AND c.name NOT IN ('','New 0x495f947276749Ce646f68AC8c248420045cb7b5eLock', 'pieceofshit', 'Uniswap V3 Positions NFT-V1','More Loot',
                    'NFTfi Promissory Note','dementorstownwtf','ShitBeast','mcgoblintownwtf','LonelyPop','Pablos','For the Culture','Hype Pass', 'Moonbirds Oddities', 'AIMoonbirds', 'Bound NFT CloneX')
    GROUP BY c.contract_id, c.name
    HAVING MAX(avg_price) > 0
    ORDER BY SUM(t.volume) DESC
    """

    # Convert the query to a Pandas DataFrame
    os_top_collection_index_df = pd.read_sql_query(os_top_collection_index, con=engine)

    # filter the query for only the top ten collections listed on OpenSea
    os_top_collection_index_df = os_top_collection_index_df[os_top_collection_index_df['name'].str.contains(
        'CryptoPunks|BoredApeYachtClub|MutantApeYachtClub|Otherdeed|Azuki|CloneX|Moonbirds|Doodles|Cool Cats|BoredApeKennelClub')==True]

    # Select only the required columns
    os_top_collection_index_df = os_top_collection_index_df[['name', 'total_volume']]

    # Rename the columns
    os_top_collection_index_df.columns = ['Collection Name', 'Volume in ETH']

    # Sort the DataFrame by Collection Name
    os_top_collection_index_df = os_top_collection_index_df.sort_values('Collection Name')

    return os_top_collection_index_df

def create_top_collections_two():
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
    os_top_collection_index_2 = os_top_collection_index_2[os_top_collection_index_2['name'].str.contains(
        'CryptoPunks|BoredApeYachtClub|MutantApeYachtClub|Otherdeed|Azuki|CloneX|Moonbirds|Doodles|Cool Cats|BoredApeKennelClub')==True]

    # Select only the required columns
    os_top_collection_index_2 = os_top_collection_index_2[['name', 'total_num_trades']]

    # Rename the columns
    os_top_collection_index_2.columns = ['Collection Name', 'Number of Trades']

    # Group the data by Collection Name and sum the number of trades
    os_top_collection_index_num_trades_df = os_top_collection_index_2.groupby('Collection Name')['Number of Trades'].sum()

    # Sort the data in alphabetical order to match the other chart
    os_top_collection_index_num_trades_df = os_top_collection_index_num_trades_df.sort_index()

    # Convert the series to a Pandas DataFrame for Streamlit Integration
    os_top_collection_index_num_trades_df = os_top_collection_index_num_trades_df.to_frame()

    # Reset the index for Streamlit Integration
    os_top_collection_index_num_trades_df.reset_index(inplace=True)

    return os_top_collection_index_num_trades_df

# Define the base time-series chart.
def get_chart(df):
    hover = alt.selection_single(
        fields=["year_month_day"],
        nearest=True,
        on="mouseover",
        empty="none",
    )

    lines = (
        alt.Chart(df, title="Average Price (ETH)")
        .mark_line()
        .encode(
            x=alt.X("yearmonthdate(year_month_day)", axis=alt.Axis(title="Month/Year")),
            y=alt.Y("avg_price", axis=alt.Axis(title="ETH")),
            color="collection",
        )
    )

    # Draw points on the line, and highlight based on selection
    points = lines.transform_filter(hover).mark_circle(size=65)

    # Draw a rule at the location of the selection
    tooltips = (
        alt.Chart(df)
        .mark_rule()
        .encode(
            x="year_month_day",
            y="avg_price",
            opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
            tooltip=[
                alt.Tooltip("year_month_day", title="Month/Year"),
                alt.Tooltip("avg_price", title="Price (ETH)"),
            ]
        )
        .add_selection(hover)
    )
    return (lines + points + tooltips).interactive()

def get_average_prices():
    sql_query = f"""
    SELECT c.name as collection,
    DATE_TRUNC('month', t.timestamp) as year_month_day,
    AVG(t.avg_price) as avg_price
    FROM trade t
    INNER JOIN collection c ON c.contract_id = t.contract_id
    INNER JOIN network n ON n.network_id = c.network_id
    WHERE n.network_id = 'ethereum' 
    AND c.name IN ('CryptoPunks', 'BoredApeYachtClub', 'MutantApeYachtClub', 'Otherdeed', 'Azuki', 'CloneX', 'Moonbirds', 'Doodles', 'Meebits', 'Cool Cats', 'BoredApeKennelClub')
    AND DATE_TRUNC('month', t.timestamp) > '2020-12-31'
    GROUP BY c.name, DATE_TRUNC('month', t.timestamp)
    HAVING MIN(avg_price) > 0.0
    ORDER BY SUM(t.volume)  DESC    
    """
    collections_df = pd.read_sql_query(sql_query, con = engine) 
    chart = get_chart(collections_df)
    # Add first annotation
    ANNOTATION1 = [
        ("Sept 30, 2021", "Best month ever for CryptoPunks! 🥳"),
    ]
    annotations1_df = pd.DataFrame(ANNOTATION1, columns=["date", "event"])
    annotations1_df.date = pd.to_datetime(annotations1_df.date)
    annotations1_df["y"] = 420
    annotation1_layer = (
        alt.Chart(annotations1_df)
        .mark_text(size=20, text="⬇", dx=-8, dy=-10, align="left")
        .encode(
            x="date:T",
            y=alt.Y("y:Q"),
            tooltip=["event"],
        )
        .interactive()
    )
    # Add second annotation
    ANNOTATION2 = [
        ("Jan 31, 2022", "Highest average price for Meebits!"),
    ]
    annotations2_df = pd.DataFrame(ANNOTATION2, columns=["date", "event"])
    annotations2_df.date = pd.to_datetime(annotations2_df.date)
    annotations2_df["y"] = 400
    annotation2_layer = (
        alt.Chart(annotations2_df)
        .mark_text(size=20, text="⬇", dx=-8, dy=-10, align="left")
        .encode(
            x="date:T",
            y=alt.Y("y:Q"),
            tooltip=["event"],
        )
        .interactive()
    )
    # Add third annotation
    ANNOTATION3 = [
        ("April 30, 2022", "NFT Market took a nose dive! 😰"),
    ]
    annotations3_df = pd.DataFrame(ANNOTATION3, columns=["date", "event"])
    annotations3_df.date = pd.to_datetime(annotations3_df.date)
    annotations3_df["y"] = 80
    annotation3_layer = (
        alt.Chart(annotations3_df)
        .mark_text(size=20, text="⬇", dx=-8, dy=-10, align="left")
        .encode(
            x="date:T",
            y=alt.Y("y:Q"),
            tooltip=["event"],
        )
        .interactive()
    )
    return (chart + annotation1_layer + annotation2_layer + annotation3_layer).interactive()