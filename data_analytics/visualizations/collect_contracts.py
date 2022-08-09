from utils import *

def generate_collections_list(path):
    """
    param path(type: str): The pathname to a text file containing collections that you would like to query
    This function takes your desired collections as input and will return a properly formatted list to use to query the data from
    Rarify. 
    """
    collections = []
    f = open(path, 'r')

    for line in f:
        line = line.rstrip()
        line = line.replace(" ", "")
        collections.append(line)

    f.close()
    return collections

def collect_addresses(key, collections_list):
    contract_addresses = {}
    for i in collections_list:
        collection = i 
        baseurl = f"https://api.rarify.tech/data/contracts?filter[name]={collection}"
        contract_addresses[i] = fetch_data_address(baseurl, key)
    return contract_addresses

def create_top_100_contracts_dict_database(sql_table):
    collections = {}
    for col in sql_table:
        try: 
            collections[col[0]] = {'name': col[2], 'network': col[3], 'unique_owners': col[5], 'tokens': col[4]}
        except Exception:
            pass
    return collections


def collect_top_100(key):
    collections = {}
    baseurl = f"https://api.rarify.tech/data/contracts/?page[limit]=10&sort=-insights.volume"
    collection_data = requests.get(
        baseurl,
        headers={"Authorization": f"Bearer {key}"}
    ).json()['data']
    for col in collection_data:
        try: 
            collections[col['attributes']['address']] = {'name': col['attributes']['name'], 'network': col['attributes']['network'], 'unique_owners': col['attributes']['unique_owners'], 'tokens': col['attributes']['tokens']}
        except Exception:
            pass
    return collections
    

        



