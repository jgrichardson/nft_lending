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

     # 06799a1e4792001aa9114f0012b9650ca28059a3
    # 1dfe7ca09e99d10835bf73044a23b73fc20623df
    # 81ae0be3a8044772d04f32398bac1e1b4b215aa8
    # 4e1f41613c9084fdb9e34e11fae9412427480e56
    # 7bd29408f11d2bfc23c34f18275bbf23bb716bc7
    # c36442b4a4522e871399cd717abdd847ab11fe88

def collect_top_100(key):
    collections = {}
    baseurl = f"https://api.rarify.tech/data/contracts/?page[limit]=50&sort=-insights.volume"
    collection_data = requests.get(
        baseurl,
        headers={"Authorization": f"Bearer {key}"}
    ).json()['data']
    for col in collection_data:
        try: 
            collections[col['attributes']['address']] = {'name': col['attributes']['name'], 'network': col['attributes']['network']}
        except Exception:
            pass
        # problem_data = ['06799a1e4792001aa9114f0012b9650ca28059a3', '1dfe7ca09e99d10835bf73044a23b73fc20623df', '81ae0be3a8044772d04f32398bac1e1b4b215aa8', '4e1f41613c9084fdb9e34e11fae9412427480e56', '7bd29408f11d2bfc23c34f18275bbf23bb716bc7', 'c36442b4a4522e871399cd717abdd847ab11fe88']
        # for item in problem_data:
        #     try:
        #         del collections[item]
        #     except: 
        #         pass
    return collections

    

        



