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

    # Row B (example of some metric widgets)
    b1, b2 = st.columns(2)

    data = api.create_nft_market_vol()
    with b1:
        st.markdown('# NFT Market Volume')
        plost.line_chart(
            data = data,
            x = 'Date',
            y = 'Volume in ETH',
            color = 'green',
            width = 500,
            height = 300,
        )        
    with st.expander("See explanation"):
        st.write("""This chart shows the total volume (in ETH) of all NFT collections traded from January 2021 to present day. You can see from January 1st, 2021 to January 31st, 2022 the NFT market grew in volume by 128,792%. The total volume of the NFT Market at its peak traded over $820 million on a single day. A line of best fit would clearly demonstrate a growing market as time progresses.""")
        
    # Row C (my sentiment plot, other plots from the team)
    # Adds an "expander" widget below each plot so we can include narrative/story
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('### Sentiment')
        st.bokeh_chart(hv.render(my_plot, backend='bokeh'))
        with st.expander("See explanation"):
            st.write("""
                Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
                Phasellus nec arcu mi. Nullam libero dui, auctor eget porta vitae, molestie quis purus. Duis malesuada arcu ex, 
                mollis ornare ante efficitur vel. Sed pulvinar erat id lectus luctus elementum. Praesent dictum, libero fermentum suscipit eleifend
            """)
    with c2:
        st.markdown('### Top Collections')
        st.table(df)
        with st.expander("See explanation"):
            st.write("""
                Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
                Phasellus nec arcu mi. Nullam libero dui, auctor eget porta vitae, molestie quis purus. Duis malesuada arcu ex, 
                mollis ornare ante efficitur vel. Sed pulvinar erat id lectus luctus elementum. Praesent dictum, libero fermentum suscipit eleifend
            """)
    
    # Row D (sample donut widget using sample collections data)
    d1, d2, d3, d4 = st.columns(4)
    with d1:
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


