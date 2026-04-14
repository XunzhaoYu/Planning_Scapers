import numpy as np
import pandas as pd
from pathlib import Path
from tools.IP_proxy import IP_list, get_IP_proxy_ip
from general.utils import get_project_root, get_data_storage_path
import os, sys
import requests

root_path = f"{Path(get_project_root()).parent}"

portal_id = int(sys.argv[1])
test_year = sys.argv[2]  # was 2013
LAs = os.listdir(f"{root_path}/Lists/")
LAs = [filename for filename in LAs if not filename.startswith('.')]
LAs.sort(key=str.lower)
LA = LAs[portal_id]

valid_IPs_folder = f"{root_path}/valid_IPs/"
#valid_IPs_path = f'{get_data_storage_path}valid_IPs/'
print(f'folder path: {valid_IPs_folder}')
if not os.path.exists(valid_IPs_folder):
    os.mkdir(valid_IPs_folder)

valid_IPs_file = f"{valid_IPs_folder}{LA}.csv"
if not os.path.exists(valid_IPs_file):
    valid_IPs = []
    for ip_index, IP in enumerate(IP_list):
        IP_proxy = get_IP_proxy_ip(ip_index)
        #print(f'IP proxy: {IP_proxy}')
        proxies = {'http': IP_proxy, 'https': IP_proxy}

        LA_df = pd.read_csv(f"{root_path}/Lists/{LA}/{LA}{test_year}.csv")
        url = LA_df.at[100, 'url']
        if LA == 'Birmingham':
            print('LA is Birmingham. load url')
            url = 'http://eplanning.idox.birmingham.gov.uk/publisher/mvc/listDocuments?identifier=Planning&reference=2000/00342/PA'
        print(f'{ip_index}, {IP}: {LA} ,{url}')
        try:
            response = requests.get(url, proxies=proxies)
            valid_IPs.append(ip_index)
            print(f'valid {ip_index}')
        except:
            #print(f'not working {ip_index}')
            pass
                
    print(f'save valid IPs: {valid_IPs}')
    IP_df = pd.DataFrame({LA: valid_IPs})
    IP_df.to_csv(valid_IPs_file, index=False)
else:
    valid_IPs_df = pd.read_csv(valid_IPs_file)#, index_col=0)
    print(f'load valid IPs: {valid_IPs_df.iloc[:, 0].tolist()}')
