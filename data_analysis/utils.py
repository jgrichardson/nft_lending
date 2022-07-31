import pandas as pd
import requests
    
def fetch_rarify_data(url, key):
    """
    param url: (type: str) The url must be supplied with a valid network_id, contract_id, and token_id
    param key: (type: str) The function returns the sale_history_data for our targeted collection at the 'history' endpoint

    """
    sale_history_data = requests.get(
        url,
        headers={"Authorization": f"Bearer {key}"}
    ).json()
    return sale_history_data['included'][1]['attributes']['history']

def fetch_data_address(url, key):
    """
    param url: (type: str) The url must be supplied with a valid network_id, contract_id, and token_id
    param key: (type: str) The function returns the sale_history_data for our targeted collection at the 'history' endpoint

    
    """
    sale_history_data = requests.get(
        url,
        headers={"Authorization": f"Bearer {key}"}
    ).json()
    return sale_history_data['data'][0]['attributes']['address']

def fetch_top_collections_data(url, key):
    """
    param url: (type: str) The url must be supplied with a valid network_id, contract_id, and token_id
    param key: (type: str) The function returns the sale_history_data for our targeted collection at the 'history' endpoint
    
    The function returns the sale_history_data for our targeted collection at the 'history' endpoint

    Will return a dictionary containing the name and address for the collection you are querying
    """
    sale_history_data = requests.get(
        url,
        headers={"Authorization": f"Bearer {key}"}
    ).json()
    
    sale_history_data = sale_history_data['data']
    data_dict = {}
    for point in sale_history_data:
        contract_address = point['attributes']['address']
        contract_name = point['attributes']['network']
        data_dict[contract_address] = contract_name
    
    return data_dict

def fetch_collections_data(contract_ids: dict, rarify_api_key: str):
    """
    :param contract_ids: (type: dict) Houses the contract addresses and the collection names
    :param rarify_api_key: (type: str) Your authentication key from the rarify API

    Takes a dictionary that houses collection addresses in the keys


    This function aggregates the data from a selection of NFT collections into a double-layered DataFrame which can be used to run a Monte Carlo simulation
    Queries 90 days of data
    
    The function iterates through the dictionary of addresses that you supply to it and makes an API call for each address.
    We then preprocess the data like we did before, formatting and setting the index as the 'time' column,
    and converting the string numbers to integers using the df.astype() method. We also convert the prices to eth from gwei using a factor. 
    We then append the most recently constructed dataframe to the list that we instantiated at the top of the function
    
    This function concatenates all the DataFrames that are present in the DataFrame list that we constructed.

    """
    df_list = []
    convert_dict = {
                    'avg_price': float,
                    'max_price': float,
                    'min_price': float,
                    'trades': float,
                    'unique_buyers': float,
                    'volume': float,
                   }  
    for address in contract_ids.keys():
        contract_id = address
        network_id = contract_ids[address]
        network_id = "ethereum"
        collections_baseurl = f"https://api.rarify.tech/data/contracts/{network_id}:{contract_id}/insights/90d"
        # curr_df = pd.DataFrame(fetch_rarify_data(collections_baseurl, rarify_api_key))
        curr_df = pd.DataFrame(fetch_raw(collections_baseurl, rarify_api_key))
        curr_df['time'] = pd.to_datetime(curr_df['time'], infer_datetime_format=True)
        curr_df = curr_df.set_index('time')
        curr_df = curr_df.astype(convert_dict)
        curr_df[['avg_price', 'max_price', 'min_price', 'volume']] = curr_df[['avg_price', 'max_price', 'min_price', 'volume']] * 10**-18
        df_list.append(curr_df)
    sum_df = pd.concat(df_list, axis=1, keys=contract_ids.keys())
    return sum_df

def fetch_top_50_collections_data(contract_ids: dict, rarify_api_key: str):
    """
    :param contract_ids: (type: dict) Houses the contract addresses and the collection names
    :param rarify_api_key: (type: str) Your authentication key from the rarify API

    Takes a dictionary that houses collection addresses in the values

    This function aggregates the data from a selection of NFT collections into a double-layered DataFrame which can be used to run a Monte Carlo simulation
    Queries all time data 
    
    The function iterates through the dictionary of addresses that you supply to it and makes an API call for each address.
    We then preprocess the data like we did before, formatting and setting the index as the 'time' column,
    and converting the string numbers to integers using the df.astype() method. We also convert the prices to eth from gwei using a factor. 
    We then append the most recently constructed dataframe to the list that we instantiated at the top of the function
    
    This function returns a concatenation of all the DataFrames that are present in the DataFrame list that we constructed.
    """
    df_list = []
    convert_dict = {
                    'avg_price': float,
                    'max_price': float,
                    'min_price': float,
                    'trades': float,
                    'unique_buyers': float,
                    'volume': float,
                   }  
    for contract_id in contract_ids.values():
        network_id = "ethereum"
        collections_baseurl = f"https://api.rarify.tech/data/contracts/{network_id}:{contract_id}/insights/all_time"
        curr_df = pd.DataFrame(fetch_rarify_data(collections_baseurl, rarify_api_key))
        curr_df['time'] = pd.to_datetime(curr_df['time'], infer_datetime_format=True)
        curr_df = curr_df.set_index('time')
        curr_df = curr_df.astype(convert_dict)
        curr_df[['avg_price', 'max_price', 'min_price', 'volume']] = curr_df[['avg_price', 'max_price', 'min_price', 'volume']] * 10**-18
        df_list.append(curr_df)
    sum_df = pd.concat(df_list, axis=1, keys=contract_ids.keys())
    return sum_df

def find_pct_change(df, contract_ids):
    """
    param df: (type: pandas.DataFrame) DataFrame must have average prices series
    param: contract_ids: (type: dict) A dictionary containing collection names in the keys 
    Will return a dataframe with percent change columns for each collection
    """
    coll_names = []
    counter = 0
    for k in contract_ids.keys():
        coll_names.append(k)
    for col in df.columns:
        if "avg_price" in col:
            df[f"{coll_names[counter]}_pct_chg"] = df[col].pct_change()
            counter += 1
    return df

def find_beta(df, contract_ids):
    betas_dict = {}
    for con in contract_ids.keys():
        # con_beta = df[f"{con}_pct_chg"].cov(df["basket_pct_chg"]) / df["basket_pct_chg"].var()
        con_beta = df[f"{con}_pct_chg"].cov(df["top_collections_basket_pct_chg"]) / df["top_collections_basket_pct_chg"].var()
        betas_dict[f"{con}_beta"] = con_beta 
    return betas_dict

def find_avg_price(input_df, contract_ids):
    df = input_df.copy()
    coll_names = [] 
    counter = 0
    for k in contract_ids.keys():
        coll_names.append(k)
    for col in df.columns:
        if "avg_price" in col:
            df[f"{coll_names[counter]}_mean_avg_price"] = df[col].mean()
            counter += 1
    return df[df.columns[-(len(contract_ids)):]]

def find_max_price(input_df, contract_ids):
    df = input_df.copy()
    coll_names = [] 
    counter = 0
    for k in contract_ids.keys():
        coll_names.append(k)
    for col in df.columns:
        if "max_price" in col:
            df[f"{coll_names[counter]}_mean_max_price"] = df[col].mean()
            counter += 1
    return df[df.columns[-(len(contract_ids)):]]

def find_min_price(input_df, contract_ids):
    df = input_df.copy()
    coll_names = [] 
    counter = 0
    for k in contract_ids.keys():
        coll_names.append(k)
    for col in df.columns:
        if "min_price" in col:
            df[f"{coll_names[counter]}_mean_min_price"] = df[col].mean()
            counter += 1
    return df[df.columns[-(len(contract_ids)):]]

def find_volume(input_df, contract_ids):
    df = input_df.copy()
    coll_names = [] 
    counter = 0
    for k in contract_ids.keys():
        coll_names.append(k)
    for col in df.columns:
        if "vol" in col:
            df[f"{coll_names[counter]}_mean_vol"] = df[col].mean()
            counter += 1
    return df[df.columns[-(len(contract_ids)):]]

def find_std_devs(input_df, contract_ids):
    df = input_df.copy()
    for coll in contract_ids.keys():
        df[f"{coll}_std_dev"] = df[f"{coll}_avg_price"].std()
    return df[df.columns[-(len(contract_ids) + 1):]].mean()

def fetch_top_collections_data(url, key):
    """
    param url: (type: str) 
    param key: (type: str)

    The following function is our base fetch for the collection data using our authorization key stored in the environment
    variables as well as the url that we supply to the function
    The url must be supplied with a valid network_id, contract_id, and token_id
    The function returns a dictionary containing the contract addresses for our specified collections
    """
    sale_history_data = requests.get(
        url,
        headers={"Authorization": f"Bearer {key}"}
    ).json()
    
    sale_history_data = sale_history_data['data']
    data_dict = {}
    for point in sale_history_data:
        contract_address = point['attributes']['address']
        contract_name = point['attributes']['network']
        data_dict[contract_address] = contract_name
    
    return data_dict

def append_collumn_names(df, contract_ids):
    cols = ["avg_price", "max_price", "min_price", "trades", "unique_buyers", "volume"]
    new_cols = []
    for key in contract_ids.keys():
        for c in cols:
            new_cols.append(f"{key}_{c}")
    new_df = df.copy()
    new_df.columns = new_cols
    return new_df 


def merge(dct_1, dct_2, dct_3):
    """
    params dct_1,2,3: (type: dict) Dictionary containing collection names and addresses

    Will return a merged dictionary of multiple dictionaries
    """
    sum_dct = {**dct_1, **dct_2, **dct_3}
    return sum_dct
    
def plot_pct_chg_collections(input_df, collection_name):
    """
    param input_df: (type: pandas.DataFrame) Must have valid percentage change columns for the collections
    param collection_name: (type: str) Must be in same format as column names in the DataFrame (Including no spaces).
    """
    return input_df[f"{collection_name}_pct_chg"].hvplot()
        