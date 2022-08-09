import os
import hvplot.pandas
import holoviews as hv
import matplotlib.pyplot as plt
from pathlib import Path
import streamlit as st
from collect_contracts import *
from utils import *
import seaborn as sns

def plot_betas():
    csv_path = Path('./betas.csv')

    beta_values = pd.read_csv(csv_path, index_col="Collections")

    plot = beta_values.tail(20).hvplot(kind="bar").opts(xrotation=90)

    return st.bokeh_chart(hv.render(plot, backend='bokeh'))


def plot_std():
    csv_path = Path('./standard_deviations.csv')

    std_devs = pd.read_csv(csv_path, index_col="Collections")

    plot = std_devs.hvplot(kind='bar', color='red').opts(xrotation=90)
    
    return st.bokeh_chart(hv.render(plot, backend='bokeh'))


def plot_index():
    csv_path = Path('./top_collections_data.csv')
    
    index = pd.read_csv(csv_path)
    keys = ['avg_price', 'min_price', 'max_price', 'volume', 'pct_chg', 'std_dev']

    index_correlation = index[keys].corr()

    return sns.heatmap(index_correlation)



def plot_mc_sim():
    csv_path = Path('./mc_cum_return.csv')

    cum_returns = pd.read_csv(csv_path)

    plot = cum_returns.hvplot()
    
    return st.bokeh_chart(hv.render(plot, backend='bokeh'))

