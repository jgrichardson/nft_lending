# Import required libraries
import re # Regex library for cleaning up Tweet text
import tweepy # Twitter API library
from tweepy import OAuthHandler # Twitter API library

def clean_tweet(tweet):
        '''
        clean tweet text by removing links, special characters using regex
        '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())
  
def get_tweet_sentiment(tweet):
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

def get_tweets(query, count):
    '''
    Main function to fetch tweets and parse them.
    '''
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