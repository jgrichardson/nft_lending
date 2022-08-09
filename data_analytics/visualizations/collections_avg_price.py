import pandas as pd
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import panel as pn
import requests
import json
from sqlalchemy import create_engine
from sqlalchemy import inspect
from collect_contracts import *
from utils import *

load_dotenv()

rarify_api_key = os.getenv("RARIFY_API_KEY")

contracts = collect_top_100(rarify_api_key)

collections_df = fetch_top_50_collections_data_api(contracts, rarify_api_key)
collections_df["b47e3cd837ddf8e4c57f05d70ab865de6e193bbb"]["avg_price"]["10-28-2021"] = 100

collections_df = avg_price_df(collections_df, contracts)

# Returns the column names from the DataFrame with tuples as column names
def extract_column_names(df):
    cols_list = []
    for col in df.columns:
        cols_list.append(col[0])
    return cols_list

# Returns a list of the collection names to rename the dataframes column
def extract_readable_names(contracts):
    names =[]
    for col in contracts:
        name = contracts[col]['name']
        names.append(name)
    return names

readable_names = extract_readable_names(contracts)
cols_list = extract_column_names(collections_df)
collections_df.columns = readable_names  
rolling_avgs = collections_df.rolling(window=20).mean()

y = pn.widgets.Select(
    name='Collection', 
    options=readable_names
)

plot = rolling_avgs.hvplot(x='time', y=y, colorbar=False, width=900, height=500, ylabel="Price (eth)")
panel = pn.Column(pn.WidgetBox(y), plot)
