from scrapy import cmdline
import re, sys

#cmdline.execute("scrapy crawl {:s} -L WARNING".format('UKPlanning_Scraper').split())
cmdline.execute(f"scrapy crawl UKPlanning_Scraper -L WARNING -a auth_index=1 -a year=-1".split())
#cmdline.execute("scrapy crawl {:s} -L WARNING".format('UKPlanning_Redownload').split())

"""  # test if IP service is working correctly.
import pprint
import requests

host = 'brd.superproxy.io'
port = 22225
username = 'brd-customer-hl_99055641-zone-datacenter_proxy1'
password = '0z20j2ols2j5'
proxy_url = f'http://{username}:{password}@{host}:{port}'

proxies = {
    'http': proxy_url,
    'https': proxy_url
}

url = "https://lumtest.com/myip.json"
response = requests.get(url, proxies=proxies)
pprint.pprint(response.json())
#"""

""" # delete empty folders.
import os
path = '../Data_Temp/'
folders = os.listdir(path)
for folder in folders:
    folder_path = path + folder
    if os.path.isdir(folder_path):
        if not os.listdir(folder_path):
            os.rmdir(folder_path)
"""

"""  # combine result.csv
import os
import pandas as pd
from tools.utils import get_data_storage_path
src_path = get_data_storage_path() + "0.results/"
filenames = os.listdir(src_path)
filenames = [src_path+filename for filename in filenames if not filename.startswith('.')]
filenames.sort(key=str.lower)
#print(filenames)
append_df = pd.concat([pd.read_csv(file) for file in filenames], ignore_index=True)
append_df.to_csv(src_path+'result.csv', index=False)
#"""

"""  # used for making 'data_to_scrape.csv'
import pandas as pd
import numpy as np
from tools.utils import get_list_storage_path
#"https://pa.bexley.gov.uk/online-applications/applicationDetails.do?activeTab=details&keyVal=0300010FUL"
auths = ['Bexley', 'Croydon', 'Newham']
#for auth in auths:
for auth in [auths[2]]:
    file_path = f"{get_list_storage_path()}{auth}/{auth}2023.csv"
    df = pd.read_csv(file_path, index_col=0)
    print(df['url'][0])
    features = np.array(df.columns.values).reshape(-1, 1)
    non_empty_counter = df.count()
    n_total = df.shape[0]
    print(np.shape(features))
    for col in range(df.shape[1]):
        if non_empty_counter.iloc[col] == n_total:
            print(f"{features[col][0]}, 100% filled")
        else:
            print(f"{features[col][0]}, {non_empty_counter.iloc[col]}/{n_total}"+" ({:.2f}%)".format(non_empty_counter.iloc[col]*100.0/n_total))

    validation = (df['last_scraped'] == df['last_different'])
    print(sum(validation))
"""





"""
For UKPlanIt_API scraper
"""
from tools.data_process import append_all, inverse_index, append_by_year
def scrape(start_index=1, end_index=424):
    cmdline.execute("scrapy crawl UKPlanIt_API -L WARNING -a start_index={:d} -a end_index={:d}".format(start_index-1, end_index).split())

#cmdline.execute("scrapy crawl {:s} -L WARNING".format('UKPlanIt_API2').split())
"""
Scrape applications from authorities by index.
Results are stored in the folder "Data_Temp"
Errors 400 and 500 can be ignored since some required datasets are not available on PlanIt API.
"""
#scrape(1, 2)

# append all:
#append_all()

# inverse_index
#inverse_index(1, 2)

# append by year
#append_by_year(1, 2)

#validate_authority(1, 2)


"""
import requests

proxypool_url = 'http://127.0.0.1:5555/random'
target_url = 'http://httpbin.org/get'

def get_random_proxy():
    #get random proxy from proxypool
    #:return: proxy
    return requests.get(proxypool_url).text.strip()

def crawl(url, proxy):
    #use proxy to crawl page
    #:param url: page url
    #:param proxy: proxy, such as 8.8.8.8:8888
    #:return: html
    proxies = {'http': 'http://' + proxy}
    return requests.get(url, proxies=proxies).text

def main():
    #main method, entry point
    #:return: none
    proxy = '181.129.183.19:53281'
    #proxy = get_random_proxy()
    print('get random proxy', proxy)
    html = crawl(target_url, proxy)
    print(html)

if __name__ == '__main__':
    main()
#"""