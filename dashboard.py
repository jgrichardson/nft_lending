# Import Required Libraries
import os # Utility library
from dotenv import load_dotenv # For loading env variables
import numpy as np
import pandas as pd
import requests
import time # For controlling rate limits to APIs

# Import libraries needed for Streamlit, and integrating plotting with Plost
import datetime as dt
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import streamlit as st
import hvplot.pandas
import holoviews as hv
import plost

# import our fetch_data module
from fetch_data import *

# import our Twitter module
from twitter import *

# Set bokeh extension, bridge between hvplot and streamlit
# In your conda env, you will need to run 'pip install --force-reinstall --no-deps bokeh==2.4.3' to support bokeh extension
hv.extension('bokeh', logo=False)

# Set the streamlit page layout to wide (reduces padding on the sides, makes page responsive)
st.set_page_config(layout="wide")

# overall class object for program
class NftLendingClient(object):
    '''
    Generic NftLending Class
    '''
    def __init__(self):
        '''
        Class constructor or initialization method
        '''
        
        # Load keys and tokens from env variables
        load_dotenv()
        twitter_bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
        meaningcloud_key = os.getenv('MEANINGCLOUD_KEY')
  
        # attempt Twitter authentication
        try:       
            self.api = tweepy.Client(bearer_token=twitter_bearer_token)
        except:
            print("Error: Authentication Failed")

def main():
    # Create object of NftLendingClient Class
    api = NftLendingClient()
    
    # Set list variable to contain Twitter hashtags for the top 10 collections by trading volume
    tag_list = ["#meebits", "#cryptopunks", "#terraforms", "#mayc", "#moonbirds", "#azuki", "#bayc", "#dreadfulz", "#clonex", "#beanz"]
    
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
    
    # Create a dataframe with response data from tweets/sentiment API (for final submission, this will be replaced with called API data above)
    
    
    
    collections = pd.read_csv('static_data/collections.csv')

    
    # Render the grid and the contents for the Streamlit dashboard
    # See https://docs.streamlit.io/library/api-reference/layout
    
    ######################## Row A ##############################
    a1, a2 = st.columns((2,8))
    with a1:
        st.image('images/nft.jpg')
    with a2:
        st.markdown('# NFT Lending Analysis')
    
    # Insert a spacer
    st.markdown('#')

    ######################## Row B ##############################
    st.header('NFT Market Growth - January 2021 to Present')
    b1, b2 = st.columns(2)
    
    # Pull the data for Row B charts
    data = create_nft_market_vol()
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

    data = create_os_collection_index()
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
            st.write("""This chart shows the total volume (in ETH) of Open Sea's Top Ten Collections traded over time. It is clear that the volume traded of the top ten collections continues to increase every time there is an interest spike in the NFT market. Additionally if you compare the top ten collections volume with the overall NFT market volume, on each peak you will find on average that Open Sea's Top Ten Collections make up 72% of the total market volume.""") 
    
    # Insert a spacer
    st.markdown('#')

    ######################## Row C ##############################
    st.header('Open Sea Top Ten Collections - All Time')
    c1, c2 = st.columns(2)
   
    # Pull the data for Row C c1 chart
    data = create_top_collections_one()
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
        st.write("""These two charts compare the total volume (in ETH) and the number of trades of Open Sea's top ten NFT collections. It is interesting to note that the all time volume for just ten NFT collections is 4,201,212ETH at today's current prices of Ether that's $7.14 Billion. Aditionally the top four collections by volume make up almost 70% of the total volume across the top ten collections, clearly demonstrating the extreme difference in value of the top 4 collections -vs- the other six. Three of the top four collections also were released by the same creators (Yuga Labs, demonstrating a concentrated and young market).""")
    
    # Pull the data for Row C c2 chart
    data = create_top_collections_two()
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
        
    # Insert a spacer
    st.markdown('#')    
        
    ######################## Row D ##############################
    st.header('Average Prices by Collection')
    st.altair_chart(
        get_average_prices(),
        use_container_width=True
    )
    with st.expander("See analysis"):
        st.write("""This chart shows the average price (in ETH) of Open Sea's Top Ten Collections traded from January 2021 to present day. You can see that there were many high points as well as several low points in the NFT market.  On September 30, 2021, CryptoPunks reached an all time high. Then on January 31, 2022, the collection Meebits reached an all time high as well.  However, on April 30, 2022 of this year the whole NFT market took a nose dive!  This very young market has definitely been volatile since it's inception!""")
    
    # Insert a spacer
    st.markdown('#')
    
    ######################## Row E ##############################
    st.header('Standard Deviation over time for Top 75 Collections')
    e1, e2 = st.columns(2)
    
    with e1:
        st.markdown('### By Volume')
        plot_std_index()
        with st.expander("See analysis.."):
            st.write("""
            Need explanation
            """)    
        
    with e2:
        st.markdown('### By Volume')
        plot_std()
        with st.expander("See analysis.."):
            st.write("""
            The standard deviation and, therefore, the volatility of some of the top collections (by percent change) are displayed here. 
            We would evaluate collections with lower standard deviations as being better candidates for collateralization and would be eligible to receive loans at a higher 
            loan-to-value ratio. This is because we would evaluate a lower risk of liquidation for these NFTs. 
            For the beta values, we find little use for this analysis because the deviation of the market as a whole is so vast and can be influenced so much 
            by the top collections it is hard to gauge the volatility of the market. Still, we see some collections that have a very low beta and we would just those as having 
            a high preference for collateralization relative to the market. 
            """)

    # Insert a spacer
    st.markdown('#')
    
    ######################## Row F ##############################
    
    st.header('Betas for Top 75 Collections')
    f1, f2 = st.columns(2)
    
    with f1:
        # st.markdown('### by Volume')
        plot_betas()
        with st.expander("See analysis.."):
            st.write("""
            We see here a wide discrepancy in Beta values. This is a good example of why it is tricky to get a good sense of volatility in such an illiquid market.
            We see that one collection, Unstoppable Domains, is such an outlier that it effects the entire basket of NFT collections. Its movements are strongly counter to the market.

            """)    
    with f2:
        # st.markdown('### Unstoppable Domains Average Price Over Time')
        plot_unstoppable_domains()
        with st.expander("See analysis.."):
            st.write("""
            If we look at the average price of the Unstoppable Domains collection we see why the data in the Betas series behaves the way it does. Unstoppable Domains has massive swings 
            where its average price fluctuates by a magnitude of thousands. With large swings like that it will affect the covariance of the entire data. 
            """)    
    
    # Insert a spacer
    st.markdown('#')
    
    ######################## Row G ##############################
    
    st.header('Simulations')
    st.markdown("### Monte Carlo Simulation For 6 Collection Portfolio Over 1 Month")
    st.markdown("Collections In this Portfolio: Bored Ape Yacht Club, Crypto Punks, Clonex, Neotokyo, Doodles, mfers")
    cum_returns = plot_mc_sim()
    plot = cum_returns.hvplot(width=1500, height=400,ylabel="Percent Increase", xlabel="Time (Days)")
    st.bokeh_chart(hv.render(plot, backend='bokeh', use_container_width=True))
    with st.expander("See analysis.."):
        st.write("""
        A portfolio consisting of these high ranking 6 collections is projected to yield high returns over the course of a month for a holder.
        You would expect to see a 30% return based on these projections if you were holding into these collections.
        """
        )
    
    # Insert a spacer
    st.markdown('#')
    
    ######################## Row H ##############################
    
    st.header('Miscellaneous Analyses')
    h1, h2 = st.columns(2)
    
    with h1:
        st.markdown("### Correlation for Statistical Measurements")
        st.image('./images/heatmap_correlation.png')
        with st.expander("See analysis.."):
            st.write("""
                Here we take a look at which statistics correlate highly to other statistics. In doing so we can gain an understanding
                of which factors we can analyze to predict the volatility of an asset. Mainly, we want to see the correlations for standard deviation and percent change.
                We see that minimum price and average price have relatively low correlations to those two but max price and volume have relatively higher ones. If we were analyzing a collections volatility,
                we could predict that maximum price and volume would have higher correlations to volatility than average price and minimum price. 
            """)    
            
    with h2:
        st.markdown('### Sentiment Analysis')
        sentiment_df = get_sentiment_data()
        sentiment_plot = sentiment_df.hvplot(x="tag", kind="bar", title="Twitter Sentiment Analysis for Top 10 NFT Collections by Trade Volume")
        st.bokeh_chart(hv.render(sentiment_plot, backend='bokeh'))
        with st.expander("See analysis.."):
            st.write("""
                This chart shows the results of an analysis of 100 recent tweets for each of the Top 10 collections. Each tweet was categorized as either Positive, Neutral, or Negative, using the Meaningcloud API.
            """)    
            
    # Insert a spacer
    st.markdown('#')
    
    # Get data and create plots
    result_df = query_correlation()
    
    chart1_df = plot_collection_max_price(result_df)
    chart2_df = plot_rarity_score(result_df)
    
    ######################## Row I ##############################
    
    st.header('Correlations of Max Price and Rarity')
    i1, i2 = st.columns(2)
    
    with i1:
        st.markdown("### Correlation of Max Price")
        chart_1 = chart1_df.hvplot.bar(
            # height=500,
            # width=1000,
            ylabel= " ETH ",
            xlabel="Collection Name",
            x='collection_name',
            y='max_price_for_collection',
            title="Price Paid of most Expensive NFT by Top 10 Collection",
            rot=90,
            color='orange'
        ).opts(yformatter='%.0f')
        
        st.bokeh_chart(hv.render(chart_1, backend='bokeh'))
        
    with i2:
        st.markdown("### Correlation of Rarity")
        chart_2 = chart2_df.hvplot.bar(
        # height=500,
        #     width=1000,
            ylabel= " Rarity Score ",
            xlabel="Collection Name",
            x='collection_name',
            title="Rarity Score of Rarest NFT by Top 10 Collection",
            rot=90,
            color='green',
        ).opts(yformatter='%.0f')
        
        st.bokeh_chart(hv.render(chart_2, backend='bokeh'))

# Call main function for program            
if __name__ == "__main__":
    # calling main function
    main()

    
