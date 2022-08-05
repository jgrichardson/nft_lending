import re
import tweepy
from tweepy import OAuthHandler
import os
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import requests
import time

import streamlit as st
import hvplot.pandas
import holoviews as hv
# You will need to run 'pip install --force-reinstall --no-deps bokeh==2.4.3' to support bokeh
hv.extension('bokeh', logo=False)

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
        # Set base URL and API key for meaningcloud
        sentiment_url = "https://api.meaningcloud.com/sentiment-2.1"
        key = os.getenv('MEANINGCLOUD_KEY')
        
        # Set payload in dict variable for passing to meaningcloud API
        payload={
            'key': key,
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
  
def main():
    # Create object of TwitterClient Class
    api = TwitterClient()
    
    # Set list variable to contain Twitter hashtags for the top 10 collections by trading volume
    #tag_list = ["#meebits", "#cryptopunks", "#terraforms", "#mayc", "#moonbirds", "#azuki", "#bayc", "#dreadfulz", "#clonex", "#beanz"]
    tag_list = ["#mayc", "#moonbirds"]
    
    # Set dict variable to contain the results
    results_dict = {
        'tag' : tag_list,
        'pos' : [],
        'neg' : [],
        'neu' : [],
    }
    
    # Iterate through hashtags
    for tag in tag_list:
    
        # Call function to get tweets
        '''
        Passing hashtag as query to Twitter API
        '''
        tweets = api.get_tweets(query = tag, count = 10)
        
        # Variables to store counts of pos, neg, and neu sentiments
        ptweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
        ntweets = [tweet for tweet in tweets if tweet['sentiment'] == 'negative']
        netweets = [tweet for tweet in tweets if tweet['sentiment'] == 'neutral']
        
        print()
        print()
        # Print out breakdown of pct of tweets for each collection that are pos, neg, and neu
        print("Positive tweets percentage: {} %".format(100*len(ptweets)/len(tweets)))
        print("Negative tweets percentage: {} %".format(100*len(ntweets)/len(tweets)))
        print("Neutral tweets percentage: {} % \
            ".format(100*(len(tweets) -(len( ntweets )+len( ptweets)))/len(tweets)))
        
        # Append count results to dict variable for later plotting
        results_dict["pos"].append(len(ptweets))
        results_dict["neg"].append(len(ntweets))
        results_dict["neu"].append(len(tweets) - len( ptweets) - len( ntweets ))

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
    
    # Create a dataframe from the results_dict
    results_df = pd.DataFrame(results_dict)
    
    # Display the results of the df and dict
    # display(results_df)
    # display(results_dict)
    
    # Plot the results
    my_plot = results_df.hvplot(x="tag", kind="bar", title="Twitter Sentiment Analysis for Top 10 NFT Collections by Trade Volume", width=1200, height=500)
    st.bokeh_chart(hv.render(my_plot, backend='bokeh'))
        
    
if __name__ == "__main__":
    # calling main function
    main()
    
    