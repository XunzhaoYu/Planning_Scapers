from scrapy import cmdline
from tools.data_process import append_all, inverse_index, append_by_year

def scrape(start_index=0, end_index=424):
    cmdline.execute("scrapy crawl UKPlanIt_API -L WARNING -a start_index={:d} -a end_index={:d}".format(start_index, end_index).split())

"""
Scrape applications from authorities by index.
Results are stored in the folder "Data"
Errors 400 and 500 can be ignored since some required datasets are not available on PlanIt API.
"""
#scrape(0, 2)

# append all:
#append_all()

# inverse_index
#inverse_index(1, 2)

# append by year
#append_by_year(1, 2)