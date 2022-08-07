import pandas as pd
import requests

def fetch_rarify_data(url, key):
    """
    The following function is our base fetch for the collection data using our authorization key stored in the environment
    variables as well as the url that we supply to the function
    The url must be supplied with a valid network_id, contract_id, and token_id
    The function returns the sale_history_data for our targeted collection at the 'history' endpoint
    """
    sale_history_data = requests.get(
        url,
        headers={"Authorization": f"Bearer {key}"}
    ).json()
    return sale_history_data['included'][1]['attributes']['history']

def fetch_top_collections_data(url, key):
    """
    The following function is our base fetch for the collection data using our authorization key stored in the environment
    variables as well as the url that we supply to the function
    The url must be supplied with a valid network_id, contract_id, and token_id
    The function returns the sale_history_data for our targeted collection at the 'history' endpoint
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

def get_collections_data(contract_ids: dict, rarify_api_key: str):
    """
    *The following function is quite messy and I will clean it up at a later time but it will work for now.*
    This function aggregates the data from a selection of NFT collections into a double-layered DataFrame which can be used to run a Monte Carlo simulation
    
    :param contract_ids: (type: dict) Houses the contract addresses and the collection names
    :param rarify_api_key: (type: str) Your authentication key from the rarify API
    
    The function iterates through the dictionary of addresses that you supply to it and makes an API call for each address.
    It then takes the relevant data and turns it into a DataFrame object.
    We then preprocess the data like we did before, formatting and setting the index as the 'time' column,
    and converting the string numbers to integers using the df.astype() method. We also convert the prices to eth from gwei using a factor. 
    We then append the most recently constructed dataframe to the list that we instantiated at the top of the function
    
    :returns: A concatenation of all the DataFrames that are present in the DataFrame list that we constructed.


    *There is obviously much more elegant way to conduct this process so let me know if you have a cleaner way of doing this*

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
        collections_baseurl = f"https://api.rarify.tech/data/contracts/{network_id}:{contract_id}/insights/90d"
        curr_df = pd.DataFrame(fetch_rarify_data(collections_baseurl, rarify_api_key))
        curr_df['time'] = pd.to_datetime(curr_df['time'], infer_datetime_format=True)
        curr_df = curr_df.set_index('time')
        curr_df = curr_df.astype(convert_dict)
        curr_df[['avg_price', 'max_price', 'min_price', 'volume']] = curr_df[['avg_price', 'max_price', 'min_price', 'volume']] * 10**-18
        df_list.append(curr_df)
    sum_df = pd.concat(df_list, axis=1, keys=contract_ids.keys())
    return sum_df

def find_pct_change(df, contract_ids):
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
        con_beta = df[f"{con}_pct_chg"].cov(df["basket_pct_chg"]) / df["basket_pct_chg"].var()
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

def fetch_top_collections_data(url, key):
    """
    The following function is our base fetch for the collection data using our authorization key stored in the environment
    variables as well as the url that we supply to the function
    The url must be supplied with a valid network_id, contract_id, and token_id
    The function returns the sale_history_data for our targeted collection at the 'history' endpoint
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