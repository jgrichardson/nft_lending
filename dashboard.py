# Import Required Libraries
import re # Regex library for cleaning up Tweet text
import tweepy # Twitter API library
from tweepy import OAuthHandler # Twitter API library
import os # Utility librarylibrary
from dotenv import load_dotenv # For loading env variables
import numpy as np
import pandas as pd
import requests
import time # For controlling rate limits to APIs
from PIL import Image # Support for images
import sqlalchemy

# Libraries needed for Streamlit, and integrating plotting with Plost
import streamlit as st
import hvplot.pandas
import holoviews as hv
import plost

# In your conda env, you will need to run 'pip install --force-reinstall --no-deps bokeh==2.4.3' to support bokeh extension
hv.extension('bokeh', logo=False)

# To run .py script in Streamlit, a requirements.txt file is needed to tell Streamlit how to set up your Python env, what libraries are needed, etc.
# In your shell run 'pip install -r requirements.txt'

# Set the streamlit page layout to wide (reduces padding on the sides, makes page responsive)
st.set_page_config(layout="wide")

class TwitterClient(object):
    '''
    Generic Twitter Class for sentiment analysis.
    '''
    def __init__(self):
        '''
        Class constructor or initialization method.
        '''
        # keys and tokens from the Twitter Dev Console
        load_dotenv()
        consumer_key = os.getenv('TWITTER_API_KEY')
        consumer_secret = os.getenv('TWITTER_SECRET_KEY')
        access_token = os.getenv('TWITTER_ACCESS_TOKEN')
        access_token_secret = os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
        twitter_bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
        meaningcloud_key = os.getenv('MEANINGCLOUD_KEY')

        # Setup the database connection (use your own .env to setup the connection)
        self.database_connection_string = os.getenv("DATABASE_URL")
        database_schema = os.getenv("DATABASE_SCHEMA")

        # create the database engine
        self.engine = sqlalchemy.create_engine(self.database_connection_string)
  
        # attempt authentication
        try:
            # This is for oAuth1 which wouldn't work with the endpoints I needed
            # create OAuthHandler object
            # self.auth = OAuthHandler(consumer_key, consumer_secret)
            # # set access token and secret
            # self.auth.set_access_token(access_token, access_token_secret)
            # # create tweepy API object to fetch tweets
            
            #oAuth2 authenticvation
            self.api = tweepy.Client(bearer_token=twitter_bearer_token)
        except:
            print("Error: Authentication Failed")
  
    def clean_tweet(self, tweet):
        '''
        clean tweet text by removing links, special characters
        using regex.
        '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())
  
    def get_tweet_sentiment(self, tweet):
        '''
        Classify sentiment of passed tweet
        '''
        # Set base URL for meaningcloud API
        sentiment_url = "https://api.meaningcloud.com/sentiment-2.1"
        
        # Set payload in dict variable for passing to meaningcloud API
        payload={
            'key': meaningcloud_key,
            'txt': self.clean_tweet(tweet),
            'lang': 'en',
        }

        # Call meaningcloud APi to get sentiment of each tweet text
        response = requests.post(sentiment_url, data=payload)
        # Parse response as json
        analysis = response.json()
    
        # Sleep for 2 second to respect API rate limits
        time.sleep(2)
        
        # Print out status code of each API call
        print('Status code:', response.status_code)
        
        # Return sentiment based on values returned
        if analysis['score_tag'] in ['P+', 'P']:
            return 'positive'
        elif analysis['score_tag'] == 'NEU':
            return 'neutral'
        else:
            return 'negative'
  
    def get_tweets(self, query, count):
        '''
        Main function to fetch tweets and parse them.
        '''
        # Create empty list variable to store parsed tweets
        tweets = []

        try:
            # call twitter api to fetch tweets
            fetched_tweets = self.api.search_recent_tweets(query=query, tweet_fields=['text'], max_results=count)
            
            # Iterate over each tweetreturned and call function to get sentiment
            for tweet in fetched_tweets.data:
                
                # Create empty dictionary variable to store required params of a tweet
                parsed_tweet = {}

                # Save text of tweet to dict variable
                parsed_tweet['text'] = tweet.text
                # Save sentiment of tweet to dict variable
                parsed_tweet['sentiment'] = self.get_tweet_sentiment(tweet.text)
                # Append parsed tweets to tweets variable
                tweets.append(parsed_tweet)

            # return parsed tweets
            return tweets

        except Exception as e:
            # print error (if any)
            print("Error : " + str(e))

    def create_nft_market_vol(self):
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
        nft_market_index_df = pd.read_sql_query(nft_market_index, con=self.engine)

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

    def create_os_collection_index(self):
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
        os_top_collection_index_df = pd.read_sql_query(os_top_collection_index, con=self.engine)

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

    def create_top_collections_one(self):
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
        os_top_collection_index_df = pd.read_sql_query(os_top_collection_index, con=self.engine)

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

        return os_top_collection_index_df

    def create_top_collections_two(self):
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
        os_top_collection_index_2 = pd.read_sql_query(os_top_collection_index_2, con=self.engine)

        # filter the query for only the top ten collections listed on OpenSea"
        os_top_collection_index_2 = os_top_collection_index_2[os_top_collection_index_2['name'].str.contains('CryptoPunks|BoredApeYachtClub|MutantApeYachtClub|Otherdeed|Azuki|CloneX|Moonbirds|Doodles|Cool Cats|BoredApeKennelClub')==True]

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

        
    
def main():
    # Create object of TwitterClient Class
    api = TwitterClient()
    
    # Set list variable to contain Twitter hashtags for the top 10 collections by trading volume
    #tag_list = ["#meebits", "#cryptopunks", "#terraforms", "#mayc", "#moonbirds", "#azuki", "#bayc", "#dreadfulz", "#clonex", "#beanz"]
    tag_list = ["#mayc", "#moonbirds"]
    
    # Set dict variable to contain the results
    # results_dict = {
    #     'tag' : tag_list,
    #     'pos' : [],
    #     'neg' : [],
    #     'neu' : [],
    # }
    
    # Iterate through hashtags
    #for tag in tag_list:
        
        # Call function to get tweets
        # '''
        # Passing hashtag as query to Twitter API
        # '''
        ### tweets = api.get_tweets(query = tag, count = 10)
        
        # Variables to store counts of pos, neg, and neu sentiments
        # ptweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
        # ntweets = [tweet for tweet in tweets if tweet['sentiment'] == 'negative']
        # netweets = [tweet for tweet in tweets if tweet['sentiment'] == 'neutral']
        
        # print()
        # print()
        # # Print out breakdown of pct of tweets for each collection that are pos, neg, and neu
        # print("Positive tweets percentage: {} %".format(100*len(ptweets)/len(tweets)))
        # print("Negative tweets percentage: {} %".format(100*len(ntweets)/len(tweets)))
        # print("Neutral tweets percentage: {} % \
        #     ".format(100*(len(tweets) -(len( ntweets )+len( ptweets)))/len(tweets)))
        
        # Append count results to dict variable for later plotting
        # results_dict["pos"].append(len(ptweets))
        # results_dict["neg"].append(len(ntweets))
        # results_dict["neu"].append(len(tweets) - len( ptweets) - len( ntweets ))

        # printing first 5 positive tweets
        #print("\n\nPositive tweets:")
        #for tweet in ptweets[:10]:
            #print(tweet['text'])

        # printing first 5 negative tweets
        #print("\n\nNegative tweets:")
        #for tweet in ntweets[:10]:
            #print(tweet['text'])

        # printing first 5 neutral tweets
        #print("\n\nNeutral tweets:")
        #for tweet in netweets[:10]:
            #print(tweet['text'])
    
    # Create a dataframe with response data from tweets/sentiment API (for final submission, this will be replaced with called API data above)
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
    results_df = pd.DataFrame(results_dict)
    
    # Create a plot for the sentiment results
    my_plot = results_df.hvplot(x="tag", kind="bar", title="Twitter Sentiment Analysis for Top 10 NFT Collections by Trade Volume")
    
    # Create a dataframe of some sample NFT collections, to simulate our "index", used to demo donut plot
    df = pd.DataFrame(np.random.randn(7, 2), columns=('Collection Name', 'Contract ID'))
    collections = pd.read_csv('collections.csv')

    # Render the grid and the contents for the Streamlit dashboard
    # See https://docs.streamlit.io/library/api-reference/layout
    
        # Row A (example of a title row)
    a1, a2 = st.columns((2,8))
    with a1:
        st.image('images/sample_logo.png')
    with a2:
        st.markdown('# NFT Lending Analysis')
    
    # Insert a spacer
    st.markdown('#')

    st.header('NFT Market Growth - January 2021 to Present')

    # Row B (General NFT Market Overview)
    b1, b2 = st.columns(2)

    data = api.create_nft_market_vol()
    with b1:
        plost.line_chart(
            data = data,
            x = 'Date',
            y = 'Volume in ETH',
            title = 'NFT Market Volume',
            color = 'green',
            width = 500,
            height = 300,
        )        
        with st.expander("See analysis"):
            st.write("""This chart shows the total volume (in ETH) of all NFT collections traded from January 2021 to present day. You can see from January 1st, 2021 to January 31st, 2022 the NFT market grew in volume by 128,792%. The total volume of the NFT Market at its peak traded over $820 million on a single day. A line of best fit would clearly demonstrate a growing market as time progresses.""")

    data = api.create_os_collection_index()
    with b2:
        plost.line_chart(
            data = data,
            x = 'Date',
            y = 'Volume in ETH',
            title = "Open Sea's Top Ten Collections Volume Traded",
            color = 'blue',
            width = 500,
            height = 300,
        )
        with st.expander("See analysis"):
            st.write("""This chart shows the total volume (in ETH) of Open Sea's Top Ten Collections traded over time. It is clear from the data that the top ten collections continue to trade at higher highs every time there is an interest spike in the NFT market.""") 
    
    # Insert a spacer
    st.markdown('#')

    st.header('Open Sea Top Ten Collections - All Time')

    # Row C (OS Top Collection Analysis)
    c1, c2 = st.columns(2)
    
    data = api.create_top_collections_one()
    with c1:
        plost.bar_chart(
            data = data,
            bar = 'Collection Name',
            value = 'Volume in ETH',
            title = "Volume in ETH",
            color = 'blue',
            width = 500,
            height = 500,
    )        
    with st.expander("See analysis"):
        st.write("""These two charts compare the total volume (in ETH) and the number of trades of Open Sea's top ten NFT collections. It is interesting to note that the all time volume for just ten NFT collections is 4,201,212ETH at today's current prices of Ether that's $7.14 Billion. Aditionally the top 4 collections by volume make up almost 70% of the total volume across the top ten collections, clearly demonstrating the extreme difference in value of the top 4 collections -vs- the other six. Three of the top four collections also were released by the same creators (Yuga Labs, demonstrating a concentrated and young market).""")
    
    data = api.create_top_collections_two()
    with c2:
        plost.bar_chart(
        data = data,
        bar = 'Collection Name',
        value = 'Number of Trades',
        title = 'Number of Trades',
        color = 'green',
        width = 500,
        height = 500,
    )
        
    # Row C (my sentiment plot, other plots from the team)
    # Adds an "expander" widget below each plot so we can include narrative/story
    d1, d2 = st.columns(2)
    with d1:
        st.markdown('### Sentiment')
        st.bokeh_chart(hv.render(my_plot, backend='bokeh'))
        with st.expander("See explanation"):
            st.write("""
                Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
                Phasellus nec arcu mi. Nullam libero dui, auctor eget porta vitae, molestie quis purus. Duis malesuada arcu ex, 
                mollis ornare ante efficitur vel. Sed pulvinar erat id lectus luctus elementum. Praesent dictum, libero fermentum suscipit eleifend
            """)
    with d2:
        st.markdown('### Top Collections')
        st.table(df)
        with st.expander("See explanation"):
            st.write("""
                Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
                Phasellus nec arcu mi. Nullam libero dui, auctor eget porta vitae, molestie quis purus. Duis malesuada arcu ex, 
                mollis ornare ante efficitur vel. Sed pulvinar erat id lectus luctus elementum. Praesent dictum, libero fermentum suscipit eleifend
            """)
    
    # Row D (sample donut widget using sample collections data)
    e1, e2, e3, e4 = st.columns(4)
    with e1:
        st.markdown('### Portfolio')
        plost.donut_chart(
            data=collections,
            theta='q2',
            color='collection')
        with st.expander("See explanation"):
            st.write("""
                Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
                Phasellus nec arcu mi. Nullam libero dui, auctor eget porta vitae, molestie quis purus. Duis malesuada arcu ex, 
                mollis ornare ante efficitur vel. Sed pulvinar erat id lectus luctus elementum. Praesent dictum, libero fermentum suscipit eleifend
            """)
    
if __name__ == "__main__":
    # calling main function
    main()


