from utils import *

def generate_collections_list(path):
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
        
        


