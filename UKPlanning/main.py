from scrapy import cmdline
import subprocess
import re, sys


import re, time
from typing import Optional


#cmdline.execute("scrapy crawl {:s} -L WARNING".format('UKPlanning_Scraper').split())
#cmdline.execute(f"scrapy crawl UKPlanning_Scraper -L WARNING -a auth_index=2 -a year=2003".split())
"""
for i in range(2021, 2022):
    #command = f"scrapy crawl UKPlanning_Scraper -L WARNING -a auth_index=0 -a year={i}"
    #command = f"scrapy crawl Atrium_Scraper -L WARNING -a auth_index=44 -a year={i}"
    #command = f"scrapy crawl PlanningExplorer_Scraper -L WARNING -a auth_index=44 -a year={i}"
    #command = f"scrapy crawl Custom_Scraper -L WARNING -a auth_index=44 -a year={i}"
    #command = f"scrapy crawl CCED_Scraper -L WARNING -a auth_index=44 -a year={i}"
    #command = f"scrapy crawl Thames_Scraper -L WARNING -a auth_index=44 -a year={i}"
    command = f"scrapy crawl Tascomi_Scraper -L WARNING -a auth_index=0 -a year={i}" # day-month-year
    subprocess.run(command.split())
#"""
command = f"scrapy crawl Tascomi_Scraper -L WARNING -a auth_index=171 -a year=-1" # day-month-year
subprocess.run(command.split())
#cmdline.execute("scrapy crawl {:s} -L WARNING".format('UKPlanning_Redownload').split())
#cmdline.execute("scrapy crawl UKPlanning_Redownload -L WARNING -a auth=Bassetlaw -a year=2008".split())


import os
import pandas as pd
pd.options.mode.chained_assignment = None
from general.utils import get_list_storage_path, get_data_storage_path
"""
path = f"{get_data_storage_path()}Bassetlaw_results.csv"
path1 = f"{get_data_storage_path()}13081-Bassetlaw-30-11-00005.csv"
path2 = f"{get_data_storage_path()}13084-Bassetlaw-46-11-00035.csv"
paths = [path, path1, path2]
df = pd.concat([pd.read_csv(file) for file in paths], ignore_index=True)
df.to_csv(f"{get_data_storage_path()}Bassetlaw_results2.csv", index=False)
"""
"""
path = f"{get_data_storage_path()}Bassetlaw_results2.csv"
for year in range(2011, 2012):
    path2 = f"{get_list_storage_path()}/Bassetlaw/Bassetlaw{year}.csv"
    scraped_dfs = pd.read_csv(path)
    #print(scraped_dfs)
    target_dfs = pd.read_csv(path2)
    #print(target_dfs)

    completed_df = pd.concat([scraped_dfs[scraped_dfs['name'] == target_dfs.iloc[index].at['name']] for index in range(len(target_dfs))], ignore_index=True)
    print(year, completed_df.shape[0], target_dfs.shape[0])
    if completed_df.shape[0] == target_dfs.shape[0]:
        completed_df.to_csv(f"{get_data_storage_path()}Bassetlaw{year}results.csv", index=False)
#"""

"""
for index in range(1, len(target_dfs)):
    app = target_dfs.iloc[index]
    #print(app.at['name'])
    scraped_app = scraped_dfs[scraped_dfs['name'] == app.at['name']]
    print(scraped_app.name)
    #try:
    #    scraped_app = scraped_dfs[scraped_dfs['name'] == app.at['name']]
    #except KeyError:
    #    print(index, app)
#"""


"""  # test if IP service changed.
import requests
headers = {'Authorization': 'Bearer 2804f004-2487-429a-a5e5-b45d52fed45f'}
#r = requests.get('https://api.brightdata.com/zone/ips/unavailable', headers=headers)
r = requests.get('https://api.brightdata.com/zone/ips?zone=datacenter_proxy1', headers=headers)
print(r.content)
#"""

# test if BrightData IP service is working correctly.
"""
import pprint
import requests
from tools.IP_proxy import IP_list
import scrapy

#headers = {'Authorization': 'Bearer 2804f004-2487-429a-a5e5-b45d52fed45f'}
#r = requests.get('https://api.brightdata.com/zone/ips/unavailable', headers=headers)
#r = requests.get('https://api.brightdata.com/zone/ips?zone=datacenter_proxy1', headers=headers)
#print(r.content)

conflict = 0
error = 0

host = 'brd.superproxy.io'
port = 22225
username = 'brd-customer-hl_99055641-zone-datacenter_proxy1'
password = '0z20j2ols2j5'

for i in range(1, len(IP_list)):
    #proxy_url = f'http://{username}:{password}@{host}:{port}'
    print(i, IP_list[i])
    proxy_url = f'http://{username}-ip-{IP_list[i]}:{password}@{host}:{port}'
    # -ip-45.139.0.240
    # -ip-80.93.202.41

    proxies = {
        'http': proxy_url,
        'https': proxy_url
    }

    url = "https://lumtest.com/myip.json"
    #url = "https://portal360.argyll-bute.gov.uk/planning/planning-documents?SDescription=04/00001/DET"
    #url = "https://upa.aberdeenshire.gov.uk/online-applications/applicationDetails.do?keyVal=QMGQUECAKX500&activeTab=summary"
    #url = "https://applications.greatercambridgeplanning.org/online-applications/applicationDetails.do?activeTab=summary&keyVal=ZZZY4YOITV277"
    #url = "https://pa.sevenoaks.gov.uk/online-applications/applicationDetails.do?activeTab=summary&keyVal=GXP68WBK53000"
    try:
        response = requests.get(url, proxies=proxies)
        pprint.pprint(response.json())
        print(response.json()['ip'])
        if response.json()['ip'] != IP_list[i]:
            conflict += 1
    except:
        error += 1
    response = requests.get(url, proxies=proxies)
    print(response)
    print(response.text)
    print(conflict, error)
    print()

print(f"conflict {conflict}, error {error}, number of IPs {len(IP_list)}")
# 36  36/ 100
# 38  38/ 100
#"""


# Test oxylabs IP proxy
"""
from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
# A package to have a chromedriver always up-to-date.
from webdriver_manager.chrome import ChromeDriverManager

USERNAME = "ispwarwick"
PASSWORD = "PLEfy7+2yWr4V=KEL"
#ENDPOINT = "pr.oxylabs.io:7777"
ENDPOINT = ["103.104.20.100:60000",
            "103.104.20.132:60000",
            "103.104.20.164:60000",
            "103.104.20.196:60000",
            "103.104.20.21:60000",

            "103.104.20.228:60000",
            "103.104.20.36:60000",
            "103.104.20.4:60000",
            "103.104.20.53:60000",
            "103.104.20.68:60000", # GB [0-9]

            "142.111.107.14:60000", # US [10-14]
            "142.111.107.174:60000",
            "142.111.107.206:60000",
            "142.111.107.238:60000",
            "142.111.107.94:60000",

            "159.148.247.10:60000", # FR [15]
            "159.148.247.42:60000", # FR [16]
            "159.148.247.74:60000", # FR [17]
            "185.201.224.40:60000", # DE [18]
            "185.201.224.5:60000", # DE [19]
            "185.201.224.8:60000", # DE [20]
            ]

#USERNAME = "xunzhao_V9W8h"
#PASSWORD = "PlanningNimbyism+2611"
ENDPOINT = ["isp.oxylabs.io:8001",
            "isp.oxylabs.io:8002",
            "isp.oxylabs.io:8003",
            "isp.oxylabs.io:8004",
            "isp.oxylabs.io:8005",
            "isp.oxylabs.io:8006",
            "isp.oxylabs.io:8007",
            "isp.oxylabs.io:8008",
            "isp.oxylabs.io:8009",
            "isp.oxylabs.io:8010",
            "isp.oxylabs.io:8011",
            "isp.oxylabs.io:8012",
            "isp.oxylabs.io:8013",
            "isp.oxylabs.io:8014",
            "isp.oxylabs.io:8015",
            "isp.oxylabs.io:8016",
            "isp.oxylabs.io:8017",
            "isp.oxylabs.io:8018",
            "isp.oxylabs.io:8019",
            "isp.oxylabs.io:8020"]

ENDPOINT = ["103.104.20.100:60000",
            "103.104.20.132:60000",
            "103.104.20.164:60000",
            "103.104.20.196:60000",
            "103.104.20.21:60000",

            "103.104.20.228:60000",
            "103.104.20.36:60000",
            "103.104.20.4:60000",
            "103.104.20.53:60000",
            "103.104.20.68:60000",  # GB [0-9]

            "103.104.20.130:60000",
            "103.104.20.162:60000",
            "103.104.20.19:60000",
            "103.104.20.194:60000",
            "103.104.20.2:60000",
            "103.104.20.226:60000",
            "103.104.20.34:60000",
            "103.104.20.51:60000",
            "103.104.20.66:60000",
            "103.104.20.98:60000"]

def chrome_proxy(user: str, password: str, endpoint: str) -> dict:
    wire_options = {
        "proxy": {
            "http": f"http://{user}:{password}@{endpoint}",
            "https": f"https://{user}:{password}@{endpoint}",
            #"http":'https://user-%s:%s@%s' % (user, password, 'isp.oxylabs.io:8001'),
            #"https": 'https://user-%s:%s@%s' % (user, password, 'isp.oxylabs.io:8001')
        }
    }
    return wire_options

def get_ip_via_chrome():
    manage_driver = Service()#executable_path=ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    #options.add_argument('--headless')
    #options.headless = False
    proxies = chrome_proxy(USERNAME, PASSWORD, ENDPOINT[15])
    driver = webdriver.Chrome(
        service=manage_driver, options=options, seleniumwire_options=proxies
    )
    try:
        #driver.get("https://ip.oxylabs.io/")
        driver.get("https://eplanning.birmingham.gov.uk/Northgate/PlanningExplorer/Generic/StdDetails.aspx?PT=PlanningApplicationsOn-Line&TYPE=PL/PlanningPK.xml&PARAM0=34613&XSLT=/Northgate/PlanningExplorer/SiteFiles/Skins/Birmingham/xslt/PL/PLDetails.xslt&FT=PlanningApplicationDetails&PUBLIC=Y&XMLSIDE=/Northgate/PlanningExplorer/SiteFiles/Skins/Birmingham/Menus/PL.xml&DAURI=PLANNING")
        #driver.get("https://pa.cheshirewestandchester.gov.uk/online-applications/applicationDetails.do?keyVal=KW8PJSEO0EB00&activeTab=summary")
        #driver.get("https://ip.oxylabs.io/location")
        ##driver.get("https://publicaccess.kingston.gov.uk/online-applications/applicationDetails.do?activeTab=summary&keyVal=ZZZZQ0NHWR579")
        #driver.get("https://papercopilot.com/statistics/iclr-statistics/iclr-2025-statistics/")
        time.sleep(60)
        return f'\nYour IP is: {re.search(r"[0-9].{2,}", driver.page_source).group()}'
    finally:
        driver.quit()

if __name__ == "__main__":
    print(get_ip_via_chrome())
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






# Preparation of designing Scrapers for new LA portals.
"""  Get the list of LAs for a given scraper type.
from tools.utils import get_scraper_by_type
scraper_type = 'Atrium'
portal_list = get_scraper_by_type(scraper_type)
#portal_list = portal_list.value
portal_list = portal_list.index
print(len(portal_list), type(portal_list))
str_list = ''
for portal in portal_list:
    #str_list = f"{str_list}, '{portal}'"
    str_list = f"{str_list}, {portal}"
str_list = str_list[2:]
print(str_list)
#"""

"""  # used for making 'data_to_scrape.csv'
import pandas as pd
import numpy as np
from tools.utils import get_list_storage_path
#"https://pa.bexley.gov.uk/online-applications/applicationDetails.do?activeTab=details&keyVal=0300010FUL"
auths = ['Bridgend', 'Cherwell', 'Crawley', 'Cumbria', 'Derbyshire', 'Essex', 'Fylde', 'Glamorgan', 'Hertfordshire', 'Kent', 'Lancashire', 'Leicester', 'Leicestershire', 'Lincolnshire', 'MalvernHills', 'Norfolk', 'NorthDevon', 'NorthumberlandPark', 'Oxfordshire', 'Redcar', 'Somerset', 'SouthWestDevon', 'Suffolk', 'Surrey', 'WelwynHatfield', 'WestmorlandFurness', 'WestNorthamptonshire', 'WestSussex', 'Worcester', 'Wychavon']
#['Bexley', 'Croydon', 'Newham']
column_values_list = []
for auth in auths:
#for auth in [auths]:
    file_path = f"{get_list_storage_path()}{auth}/{auth}2018.csv"
    df = pd.read_csv(file_path, index_col=0)
    #print(df.columns.values)
    def count_if_filled():
        print(df['url'][0])
        features = np.array(df.columns.values).reshape(-1, 1)
        print(np.shape(features))
        print(features)

        non_empty_counter = df.count()  # count the non-empty cells for each column
        n_total = df.shape[0]
        for col in range(df.shape[1]):
            if non_empty_counter.iloc[col] == n_total:
                print(f"{features[col][0]}, 100% filled")
            else:
                print(f"{features[col][0]}, {non_empty_counter.iloc[col]}/{n_total}"+" ({:.2f}%)".format(non_empty_counter.iloc[col]*100.0/n_total))

        validation = (df['last_scraped'] == df['last_different'])
        print(sum(validation))
    column_values = list(df.columns.values)
    column_values_list.append(column_values)
    print(np.shape(column_values), type(column_values))

columns_union = column_values_list[0]
for df_columns in column_values_list[1:]:
    columns_union = list(set().union(columns_union, df_columns))

print('columns union:', np.shape(columns_union), type(columns_union))
print(columns_union)
#"""










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
#index = 267  # [18, 28, 87, 93, 107, 129, 141, 399, 407, 408] diff pages. (index starts from 0)
#print(f'index: {index}')
#scrape(index, index)
#scrape(49, 49) # 2023: 29，33，64(not exist)，39， 52(1 empty) now 62
### --- 2024 ---
### Two names, access to the same data with either name.
# 33(Blaenau BlaenauGwent*), 64(Carlisle* Carlisle2), 98(Denbighshire* Denbighshire2), 106(Doncaster* Doncaster2), 118(EastHampshire* EastHants),
# 189(IOW* IsleOfWight), 240(NewcastleUnderLyme* NewcastleUnderLyme2), 327(SouthCambridgeshire SouthCambs*),
# 379(TowerHamlets* TowerHamlets2), 403(WestNorthamptonshire* WestNorthants), 423(WyreForest WyreForestDC*)
### Empty file:
# No apps in the given months: 52(Buckinghamshire), 104(DFISPD), 105(DNS), 177(Hertfordshire), 247(NIP), 418(Worcestershire)
# UKPlanIt did not update (all): 39(Braintree), 165(Hampshire), 271(Nuneaton), 287(Preston), 358(Surrey)
# UKPlanIt did not update (part): 60(Camden 7.3), 216(LondonLegacy 8.31), 248(NIPW 2.22), 371(Tewkesbury 8.16)
# LA not exist : 310(Sark)
# Not included in scraper_name_2025.csv: Poole, Bournemouth, Christchurch
### Done:
# Multi pages: 86(Cornwall)
# Was multi-page for previous years:
#   19(Barnet), 29(Birmingham), 92(Croydon), 107(DorsetCouncil), 130(Edinburgh), 142(FermanaghOmagh),
#   198(Kingston), 205(Leeds), 209(Lewisham)，235(MoleValley), 401(Westminster), 409(Wigan), 410(Wiltshire)
# 259(Norfolk), 320(Shropshire), 323(Solihull), 387(Warrington)


# append all
# append_all(temp=False)

"""
check_list = [] # 60Cambridgeshire, 102Derbyshire, 105Devon, 179Hertfordshire, 249NIP, 261NorthNorthants, 269NorthYorkshire
# Braintree, 167Hampshire, 273Nuneaton, 290(Preston), 360(Surrey)
import pandas as pd
from tools.utils import get_list_storage_path, get_data_storage_path, get_csv_files
for dir in range(0, 428):
    csv_files = get_csv_files(get_list_storage_path(), start_dir=dir, end_dir=dir + 1)
    str = csv_files[0].split('/')[-1]

    n_years = 2024 - int(str[-8:-4]) + 1
    print(dir+1, str, n_years, len(csv_files))
    if n_years != len(csv_files):
        print('--- --- --- --- --- --- --- --- --- --- --- --- ---')
        check_list.append(dir+1)
print(check_list)
#"""

# inverse_index
# inverse_index(1, 69)  # modify 'utils.py: get_data_storage_path' before use this function.

# append by year
# append_by_year(58, 58) # modify 'utils.py: get_data_storage_path and get_list_storage_path' before use this function.

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