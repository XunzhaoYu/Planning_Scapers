from scrapy import cmdline
from tools.data_process import append_all, inverse_index, append_by_year
import re

def scrape(start_index=1, end_index=424):
    cmdline.execute("scrapy crawl UKPlanIt_API -L WARNING -a start_index={:d} -a end_index={:d}".format(start_index-1, end_index).split())

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

"""
document_info = 'Correspondence for discharge of condition 53530 details of methodology and painting scheme - acceptable 17/02/2011 0 0 1.0.0 .msg'
day = '[0-9]{2}'
month = '\w*'
year = '[0-9]{4}'
pattern_date = f'{day}/{month}/{year}'
date = re.search(pattern_date, document_info, re.I).group()
print(type(date), date)
document_info = document_info.split(date)[0]
case_num = re.search('\d+', document_info, re.I).group()
print(type(case_num), case_num)
document_info = document_info.split(case_num)
document_type = document_info[0].strip()
document_description = document_info[1].strip()
print(type(document_type), document_type)
print(type(document_description), document_description)
"""
#"""
#cmdline.execute("scrapy crawl {:s} -L WARNING".format('UKPlanIt_API2').split())
cmdline.execute("scrapy crawl {:s} -L WARNING".format('UKPlanning_Scraper').split())

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
from tools.utils import get_temp_storage_path
src_path = get_temp_storage_path() + "0.results/"
filenames = os.listdir(src_path)
filenames = [src_path+filename for filename in filenames if not filename.startswith('.')]
filenames.sort(key=str.lower)
#print(filenames)
append_df = pd.concat([pd.read_csv(file) for file in filenames], ignore_index=True)
append_df.to_csv(src_path+'result.csv', index=False)
#"""


"""
import pandas as pd
import numpy as np
from tools.utils import get_storage_path
#"https://pa.bexley.gov.uk/online-applications/applicationDetails.do?activeTab=details&keyVal=0300010FUL"
auths = ['Bexley', 'Croydon', 'Newham']
#for auth in auths:
for auth in [auths[2]]:
    file_path = f"{get_storage_path()}{auth}/{auth}2023.csv"
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

