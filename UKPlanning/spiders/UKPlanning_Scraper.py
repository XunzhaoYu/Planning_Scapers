import scrapy
from scrapy import signals
from scrapy import Request
#from scrapy.spiders import CrawlSpider, Rule
#from scrapy.linkextractors import LinkExtractor
from items import DownloadFilesItem
from settings import PRINT, CLOUD_MODE, DEVELOPMENT_MODE
#import requests
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
#import csv
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
#from tools.bypass_reCaptcha import bypass_reCaptcha
from tools.utils import get_project_root, get_list_storage_path, get_data_storage_path, get_filenames, Month_Eng_to_Digit, get_scraper_by_type
from tools.curl import upload_file, upload_folder
from tools.email_sender import send_emails
import time, random, timeit, re, os, sys
import zipfile
import difflib  # for UPRN
import warnings
from datetime import datetime
import pprint

class UKPlanning_Scraper(scrapy.Spider):
    name = 'UKPlanning_Scraper'

    """ for testing runtime
    for i in range(10):
        t1 = timeit.timeit(setup='import pandas as pd; '
                           'from tools.utils import get_list_storage_path; '
                           'auth = "Bexley";'
                           'file_path = f"{get_list_storage_path()}{auth}/{auth}2011.csv"; '
                           'df = pd.read_csv(file_path, index_col=0); '
                           'app_df = df.iloc[5]',
                     stmt='app_df["description"]="dec"', number=10000)

        t2 = timeit.timeit(setup='import pandas as pd; '
                           'from tools.utils import get_list_storage_path; '
                           'auth = "Bexley";'
                           'file_path = f"{get_list_storage_path()}{auth}/{auth}2011.csv"; '
                           'df = pd.read_csv(file_path, index_col=0); '
                           'app_df = df.iloc[5]',
                     stmt='app_df.at["description"]="dec"', number=10000)
        print(t1, t2, "{:.2f}%".format(t2*100.0/t1))
    #"""

    Non_empty = ['uid', 'scraper_name', 'url', 'link', 'area_id', 'area_name'] + \
                ['last_scraped', 'last_different', 'last_changed', 'other_fields.comment_url']  # 10 = 6 + 4
    Locations = ['location', 'location_x', 'location_y', 'other_fields.easting', 'other_fields.lat', 'other_fields.latitude'] + \
                ['other_fields.lng', 'other_fields.longitude', 'other_fields.northing']  # 9 = 6 + 3

    # self.app_df 81 - 19 + 8 = 70
    # Summary: 10 + 2*
    summary_dict = {'Reference': 'uid',  # Non-Empty
                    'Application Reference': 'uid',  # New Duplicate [Derby]
                    'Planning Portal Reference': 'other_fields.planning_portal_id',  # New [Derby]
                    'Alternative Reference': 'altid',
                    #
                    'Application Received': 'other_fields.date_received',
                    'Application Received Date': 'other_fields.date_received',  # New Duplicate [Chelmsford]
                    'Application Registered': 'other_fields.date_received',  # New Duplicate [Rhondda]
                    'Application Validated': 'other_fields.date_validated',
                    #
                    'Address': 'address',
                    'Location': 'address',  # Duplicate [Derby]
                    'Proposal': 'description',
                    'Status': 'other_fields.status',
                    'Decision': 'other_fields.decision',
                    'Decision Issued Date': 'other_fields.decision_issued_date',
                    'Appeal Status': 'other_fields.appeal_status',
                    'Appeal Decision': 'other_fields.appeal_result',
                    'Local Review Body Status': 'other_fields.local_review_body_status',  # New*
                    'Local Review Body Decision': 'other_fields.local_review_body_decision'  # New*
                    }
    # Further Information: 10 + 3 + 2*
    details_dict = {'Application Type': 'other_fields.application_type',
                    'Decision': 'other_fields.decision',  # Duplicated in summary
                    'Actual Decision Level': 'other_fields.actual_decision_level',  # New
                    'Expected Decision Level': 'other_fields.expected_decision_level',  # New
                    'Decision Level': 'other_fields.expected_decision_level',  # New Duplicate [Moray]
                    #
                    'Case Officer': 'other_fields.case_officer',
                    'Parish': 'other_fields.parish',
                    'Amenity Society': 'other_fields.amenity_society', # New [Westminster]
                    'Ward': 'other_fields.ward_name',
                    'District Reference': 'other_fields.district',
                    'Applicant Name': 'other_fields.applicant_name',
                    'Applicant Address': 'other_fields.applicant_address',
                    'Agent Name': 'other_fields.agent_name',
                    'Agent Company Name': 'other_fields.agent_company',
                    'Agent Phone Number': 'other_fields.agent_phone',  # New*
                    'Agent Address': 'other_fields.agent_address',
                    'Environmental Assessment Requested': 'other_fields.environmental_assessment',  # New
                    'Environmental Assessment Required': 'other_fields.environmental_assessment',  # New Duplicate [Perth]
                    'Community Council': 'other_fields.community_council',  # New*
                    'Community': 'other_fields.community_council',  # New* Duplicate [BreconBeacons]
                    'Community/Town Council': 'other_fields.community_council',  # New* Duplicate [Caerphilly]
                    }
    # Important Datas: 14 + 4 + 1*
    dates_dict = {'Application Received Date': 'other_fields.date_received',  # Duplicated in summary
                  'Application Validated Date': 'other_fields.date_validated',  # Duplicated in summary
                  'Date Application Valid': 'other_fields.date_validated',  # Duplicated in summary [NewcastleUnderLyme]
                  'Application Valid Date': 'other_fields.date_validated',  # Duplicated in summary [Oadby]
                  'Valid Date': 'other_fields.date_validated',  # New Duplicated in summary [EastHampshire]

                  'Expiry Date': 'other_fields.application_expires_date',
                  'Application Expiry Date': 'other_fields.application_expires_date',  # New Duplicate [MiltonKeynes]
                  'Application Expiry Deadline' :  'other_fields.application_expires_date',  # New Duplicate [Sefton]
                  'Statutory Expiry Date': 'other_fields.statutory_expires_date',  # New []
                  #
                  'Expiry Date for Comment': 'other_fields.comment_expires_date',  # New
                  'Expiry Date for Comments': 'other_fields.comment_expires_date',  # New Duplicate [Moray]
                  'Last Date For Comments': 'other_fields.comment_expires_date',  # New Duplicate [Edinburgh]
                  'Last Date for Comments': 'other_fields.comment_expires_date',  # New Duplicate [Glasgow]
                  'Last date for public comments': 'other_fields.comment_expires_date',  # New Duplicate [Perth]
                  'Comments To Be Submitted By': 'other_fields.comment_expires_date',  # New Duplicate [Leeds]
                  #
                  'Actual Committee Date': 'other_fields.meeting_date',
                  'Committee Date': 'other_fields.meeting_date',  # New Duplicate [Chelmsford]
                  'Actual Committee or Panel Date': 'other_fields.meeting_date',  # New Duplicate [Gedling]
                  #
                  'Latest Neighbour Consultation Date': 'other_fields.neighbour_consultation_start_date',
                  'Neighbour Consultation Expiry Date': 'other_fields.neighbour_consultation_end_date',
                  'Neighbour Notification Expiry Date': 'other_fields.neighbour_consultation_end_date',  # New Duplicate [Sefton]
                  'Standard Consultation Date': 'other_fields.consultation_start_date',
                  'Standard Consultation Expiry Date': 'other_fields.consultation_end_date',
                  'Consultation Expiry Date': 'other_fields.consultation_end_date',  # New Duplicate [Chelmsford]
                  'Consultation Deadline': 'other_fields.consultation_end_date',  # New Duplicate [NorthSomerest]
                  'Public Consultation Expiry Date':  'other_fields.consultation_end_date',  # New Duplicate [Oadby]
                  'Consultation Period To End On': 'other_fields.consultation_end_date',  # New Duplicate [Torbay]

                  'Overall Consultation Expiry Date': 'other_fields.overall_consultation_expires_date',  # New []
                  'Overall Date of Consultation Expiry': 'other_fields.overall_consultation_expires_date',  # New Duplicate []
                  #
                  'Last Advertised In Press Date': 'other_fields.last_advertised_date',
                  'Advertised in Press Date': 'other_fields.last_advertised_date', # New Duplicate [Glasgow]
                  'Latest Advertisement Expiry Date': 'other_fields.latest_advertisement_expiry_date',
                  'Advertisement Expiry Date': 'other_fields.latest_advertisement_expiry_date',  # New Duplicate [NorthHertfordshire]
                  #
                  'Last Site Notice Posted Date': 'other_fields.site_notice_start_date',
                  'Latest Site Notice Expiry Date': 'other_fields.site_notice_end_date',
                  'Site Notice Expiry Date': 'other_fields.site_notice_end_date', # New Duplicate [NorthHertfordshire]
                  #
                  'Internal Target Date': 'other_fields.target_decision_date',
                  'Agreed Expiry Date': 'other_fields.agreed_expires_date',  # New
                  'Decision Made Date': 'other_fields.decision_date',
                  'Decision Issued Date': 'other_fields.decision_issued_date',  # Duplicated in summary
                  'Permission Expiry Date': 'other_fields.permission_expires_date',
                  'Decision Printed Date': 'other_fields.decision_published_date',
                  'Decision Due Date': 'other_fields.decision_due_date',  # New [Chelmsford]
                  'Environmental Impact Assessment Received': 'other_fields.environmental_assessment_date',  # New
                  'Determination Deadline': 'other_fields.determination_date',  # New
                  'Statutory Determination Deadline': 'other_fields.statutory_determination_deadline',  # New []
                  'Statutory Determination Date': 'other_fields.statutory_determination_deadline',  # New Duplicate [Oadby]
                  'Extended Determination Deadline': 'other_fields.extended_determination_deadline',  # New [NorthSomerest]
                  'Temporary Permission Expiry Date': 'other_fields.temporary_permission_expires_date',  # New
                  'Local Review Body Decision Date': 'other_fields.local_review_body_decision_date'  # New*
                  }

    # FOR_IP_TEST_ONLY
    """
    proxy_host = 'brd.superproxy.io'
    proxy_port = 22225
    proxy_username = 'brd-customer-hl_99055641-zone-datacenter_proxy1'
    proxy_password = '0z20j2ols2j5'
    #"""

    def handle_error_log(self):
        if os.path.exists(f"{get_data_storage_path()}error_log_temp.txt"):
            if os.path.exists(f"{get_data_storage_path()}error_log.txt"):
                error_log = open(f"{get_data_storage_path()}error_log.txt", "a")
                error_log_temp = open(f"{get_data_storage_path()}error_log_temp.txt", "r").read()
                error_log.write('\n \n \n \n \n' + error_log_temp)
                error_log.close()
            else:
                os.rename(f"{get_data_storage_path()}error_log_temp.txt", f"{get_data_storage_path()}error_log.txt")

    def __init__(self, auth_index, year):
        super().__init__()
        self.start_time = time.time()
        self.data_storage_path = get_data_storage_path()
        self.year = year
        print(self.year)

        # for testing some samples from an authority
        #if True:
        #    pass
        if DEVELOPMENT_MODE:
            auth_names = get_scraper_by_type()
            auth_names = [auth_name for auth_name in auth_names if not auth_name.startswith('.')]
            auth_names.sort(key=str.lower)
            print(auth_names)
            # auth_names = auth_names[[0, 1, 2, 3[ExternalDoc], 4, 6, 7, 8[2003-2022], 9[no 2016], 11, 12
            # 10[too many requests], 13[too many requests], 14, 17, 19]]
            app_dfs = []
            self.auth_index = 5 # 59, 239
            # IP rotations: 5, 7, 33, 35, 43;   51, 59*[8 times], 67, 77, 104;   118, 121, 131[no comment], 142, 153;
            # 161, 178, 183[no comment], 184, 186;   190, 200, 218, 223, 225;   230, 243[no comment],
            """
            4, 47, 92, 98 for 5 
            work: 
            80.93.198.225, 80.93.199.133, 80.93.199.180, 80.93.199.200, 80.93.201.107, 80.93.202.6, 80.93.203.211, 80.93.204.168, 80.93.204.175, 80.93.205.206, 80.93.206.43, 80.93.206.231
            89.40.209.6, 89.40.209.69, 89.40.209.73, 89.40.209.129
            92.255.80.47
            121.91.87.39
            195.210.97.159
            217.9.18.94
            not:  
            45.66.179.164
            92.43.86.48
            103.109.80.101, 103.109.81.77, 103.109.81.96, 103.109.81.132
            110.239.209.27, 110.239.214.37
            188.190.106.151, 188.190.123.124
            193.42.59.216, 193.56.24.5
            """
            self.year = -1
            self.auth = auth_names[int(self.auth_index)]
            self.data_storage_path = f"{self.data_storage_path}{self.auth}/{self.year}/"
            self.data_upload_path = f"{self.auth}/{self.year}/"
            if not os.path.exists(f"{get_data_storage_path()}{self.auth}"):
                os.mkdir(f"{get_data_storage_path()}{self.auth}")
                upload_folder(f"{self.auth}") if CLOUD_MODE else None
            if not os.path.exists(self.data_storage_path):
                os.mkdir(self.data_storage_path)
                upload_folder(self.data_upload_path) if CLOUD_MODE else None

            #years = np.linspace(2002, 2021, 20, dtype=int)
            #years = np.linspace(2003, 2022, 20, dtype=int)  # 11
            # years = np.append(years[:14], years[15:])
            # 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021
            sample_index = 5
            for auth in auth_names[self.auth_index:self.auth_index + 1]:  # 5, 15, 16, 18
                #for year in years:
                #    file_path = f"{get_list_storage_path()}{auth}/{auth}{year}.csv"
                filenames = get_filenames(f"{get_list_storage_path()}{auth}/")
                print(f"{auth}. number of files: {len(filenames)}")
                for filename in filenames[2:]:
                    file_path = f"{get_list_storage_path()}{auth}/{filename}"
                    df = pd.read_csv(file_path)  # , index_col=0)  # <class 'pandas.core.frame.DataFrame'>
                    print(filename, df.shape[0])
                    # app_df = df.iloc[[sample_index], :]
                    app_df = df.iloc[sample_index, :]
                    # print(pd.DataFrame(app_df).T)
                    app_dfs.append(app_df)
            self.app_dfs = pd.concat([pd.DataFrame(app_df).T for app_df in app_dfs], ignore_index=True)
        else:
            auth_names = os.listdir(get_list_storage_path())
            auth_names = [auth_name for auth_name in auth_names if not auth_name.startswith('.')]
            auth_names.sort(key=str.lower)
            self.auth = auth_names[int(auth_index)]
            # option1: scrape all years
            year = int(self.year)
            if year < 0:
               src_path = f"{get_list_storage_path()}{self.auth}/"
               src_filenames = os.listdir(src_path)
               src_filenames.sort(key=str.lower)
               #src_files = []
               #for filename in src_filenames:
               #    if not filename.startswith('.'):
               #        src_files.append(src_path+filename)
               src_files = [src_path+filename for filename in src_filenames if not filename.startswith('.')]
               self.app_dfs = pd.concat([pd.read_csv(file) for file in src_files], ignore_index=True)
            # option2: scrape a given year
            else:
                src_path = f"{get_list_storage_path()}{self.auth}/{self.auth}{self.year}.csv"
                self.app_dfs = pd.read_csv(src_path)

            self.data_storage_path = f"{self.data_storage_path}{self.auth}/{self.year}/"
            self.data_upload_path = f"{self.auth}/{self.year}/"
            if not os.path.exists(f"{get_data_storage_path()}{self.auth}"):
                os.mkdir(f"{get_data_storage_path()}{self.auth}")
                upload_folder(f"{self.auth}") if CLOUD_MODE else None
            if not os.path.exists(self.data_storage_path):
                os.mkdir(self.data_storage_path)
                upload_folder(self.data_upload_path) if CLOUD_MODE else None

            # read the list of scraping.
            self.list_path = f"{self.data_storage_path}to_scrape_list.csv"
            if not os.path.isfile(self.list_path):
                self.init_index = 0  #2173 if int(self.year)==2003 else 2132 #1004
                self.to_scrape = self.app_dfs.iloc[self.init_index:, 0]
                self.to_scrape.to_csv(self.list_path, index=True)
                print("write", self.to_scrape)
            else:
                self.to_scrape = pd.read_csv(self.list_path, index_col=0)
                if self.to_scrape.empty:
                    sys.exit("To_scrape_list is empty.")
                else:
                    print("read", self.to_scrape)
            self.app_dfs = self.app_dfs.iloc[self.to_scrape.index,:]
        print(self.app_dfs)

        # settings.
        self.index = -1
        #self.index = self.init_index
        self.failures = 0
        #self.failed_apps = []

        # record data
        self.result_storage_path = f"{self.data_storage_path}0.results/"
        if not os.path.exists(self.result_storage_path):
            os.mkdir(self.result_storage_path)
        # record failures and errors.
        self.failed_downloads_path = f"{self.data_storage_path}failed_downloads/"
        if not os.path.exists(self.failed_downloads_path):
            os.mkdir(self.failed_downloads_path)
        if CLOUD_MODE:
            self.failed_uploads_path = f"{self.data_storage_path}failed_uploads/"
            if not os.path.exists(self.failed_uploads_path):
                os.mkdir(self.failed_uploads_path)
        #self.handle_error_log()

        # allowed_domains = ['pa.bexley.gov.uk']
        # start_urls = ['https://pa.bexley.gov.uk/online-applications/applicationDetails.do?keyVal=LELZV9BE01D00&activeTab=summary']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UKPlanning_Scraper, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.idle_consume, signals.spider_idle)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def idle_consume(self):
        reqs = self.start_requests()
        if not reqs:
            return
        for req in reqs:
            #self.crawler.engine.schedule(req, self)
            self.crawler.engine.crawl(req)
        #raise DontCloseSpider
        # ScrapyDeprecationWarning: ExecutionEngine.schedule is deprecated, please use ExecutionEngine.crawl or ExecutionEngine.download instead self.crawler.engine.schedule(req, self)

    def spider_closed(self, spider):
        # summarize scraped data:
        filenames = os.listdir(self.result_storage_path)
        filenames.sort(key=str.lower)
        files = [self.result_storage_path + filename for filename in filenames if not filename.startswith('.')]
        append_df = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
        current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        if DEVELOPMENT_MODE:
        # developing scrapers for new authorities
            append_df.to_csv(self.data_storage_path + f'result_{current_time}.csv', index=False)
            #upload_file(f'result_{current_time}.csv') if CLOUD_MODE else None
        else:
            filenames = os.listdir(self.data_storage_path)
            pattern = r'\w*_result_\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}.csv'
            for filename in filenames:
                match = re.search(pattern, filename, re.I)
                if match:
                    previous_result_path = f"{self.data_storage_path}{match.group()}"
                    os.remove(previous_result_path)
            append_df.to_csv(self.data_storage_path + f'{self.auth}_result_{current_time}.csv', index=False)
            #upload_file(f'{self.auth}_result_{current_time}.csv') if CLOUD_MODE else None
            #send_emails(self.auth)

        """
        # summarize error logs:
        try:
            self.handle_error_log()
        except FileNotFoundError:
            print('Cannot find error log.')
        #"""
        time_cost = time.time() - self.start_time
        print("final time_cost: {:.0f} mins {:.4f} secs.".format(time_cost // 60, time_cost % 60))

    def start_requests(self):
        # parallel, discarded.
        """
        #for index, app_df in enumerate(self.app_dfs[self.init_index:]):
        for index in range(self.app_dfs.shape[0]):
            app_df = self.app_dfs.iloc[index, :]
            url = app_df.at['url']
            print(f"\n{app_df.name}, start url: {url}")
            print(app_df) if PRINT else None
            #yield scrapy.Request(url=url, callback=self.parse_item)
            yield SeleniumRequest(url=url, callback=self.parse_summary_item, meta={'app_df':app_df})
        """
        # sequential.
        self.index += 1
        """ # FOR_IP_TEST_ONLY
        url = "http://lumtest.com/myip.json"
        print("url:", url)
        yield SeleniumRequest(url=url, callback=self.parse_IP)
        """
        try:
            app_df = self.app_dfs.iloc[self.index, :]
            url = app_df.at['url']
            print(f"\n{app_df.name}, start url: {url}")
            while type(url) != str:
                self.index += 1
                app_df = self.app_dfs.iloc[self.index, :]
                url = app_df.at['url']
                print(f"\n{app_df.name}, start url: {url}")
            print(app_df) if PRINT else None
            # yield scrapy.Request(url=url, callback=self.parse_item)
            yield SeleniumRequest(url=url, callback=self.parse_summary_item, meta={'app_df': app_df})
        except IndexError:
            print("list is empty.")
            return
        #"""

    """
    Testing Functions
    """
    # FOR_IP_TEST_ONLY
    def start_requests_FOR_IP_TEST_ONLY(self):
        #url = "https://www.whatismyip.com/"
        url = "http://lumtest.com/myip.json"
        #IP_proxy = f'http://{self.proxy_username}:{self.proxy_password}@{self.proxy_host}:{self.proxy_port}'
        #print("1", IP_proxy)
        # {brd.superproxy.io:22225}:{brd-customer-hl_99055641-zone-datacenter_proxy1}-ip-188.190.122.220:{0z20j2ols2j5}
        yield SeleniumRequest(url=url, callback=self.parse_IP)
        #yield Request(url="http://lumtest.com/myip.json", callback=self.parse_IP, meta={'proxy': IP_proxy})

    # FOR_IP_TEST_ONLY
    def parse_IP(self, response):
        #pprint.pprint(response.json())  # for Request
        print(response.xpath('/html/body/pre/text()').get()[1:24].split(',')[0])  # for SeleniumRequest
        print('----- ----- -----')
        #url = "https://www.github.com/"
        url = 'https://publicaccess.aylesburyvaledc.gov.uk/online-applications/applicationDetails.do?activeTab=summary&keyVal=H6JFZ4CL40000'
        yield SeleniumRequest(url=url, callback=self.parse_IP2)
        """
        app_df = self.app_dfs.iloc[self.index, :]
        url = app_df.at['url']
        print(f"\n{app_df.name}, start url: {url}")
        print(app_df) if PRINT else None
        yield SeleniumRequest(url=url, callback=self.parse_summary_item, meta={'app_df': app_df})
        #"""

    def parse_IP2(self, response):
        #print(response.body)
        pass

    """
    Auxiliary Functions
    """
    def is_empty(self, cell):
        return pd.isnull(cell)

    def convert_date(self, date_string):
        strs = date_string.split(' ')
        if len(strs) > 2:
            year = strs[3]
            month = Month_Eng_to_Digit(strs[2])
            day = strs[1]
            return f"{year}-{month}-{day}"
        else:
            return date_string

    def upload_and_delete(self, folder_name, file_name):
        if upload_file(f"{self.data_upload_path}{folder_name}/{file_name}") == 0:
            os.remove(f"{self.data_storage_path}{folder_name}/{file_name}")

    def get_doc_url(self, response, app_df):
        tab_lis = response.xpath('//*[@id="pa"]/div[3]/div[3]/ul').xpath('./li')
        url = ''
        for li in tab_lis:
            try:  # tab has a link.
                tab_name = li.xpath('./a/span/text()').get()
                if tab_name and 'document' in str.lower(tab_name):
                    url = li.xpath('./a/@href').get()
                    url = response.urljoin(url)
                    break
            except TypeError:  # tab has no link.
                tab_name = li.xpath('./span/text()').get()
                if tab_name and 'document' in str.lower(tab_name):
                    url = app_df.at['url'].replace('summary', 'documents')
                    break
        if url == '':
            url = app_df.at['url'].replace('summary', 'documents')
        app_df.at['other_fields.docs_url'] = url
    """
    Parse Functions
    """
    def parse_summary_item(self, response):
        #driver = response.request.meta["driver"]
        app_df = response.meta['app_df']
        self.get_doc_url(response, app_df)
        items = response.xpath('//*[@id="simpleDetailsTable"]/tbody/tr')
        n_items = len(items)
        print(f"\nSummary Tab: {n_items}") if PRINT else None #print(f"Summary Tab: {n_items}")
        for item in items:
            item_name = item.xpath('./th/text()').get().strip()
            data_name = self.summary_dict[item_name]

            #if data_name in self.app_dfs.columns:
            try:
                # Empty
                if self.is_empty(app_df.at[data_name]):
                    # Date
                    if item_name in ['Application Received', 'Application Received Date', 'Application Registered',
                                     'Application Validated', 'Decision Issued Date']:
                        date_string = item.xpath('./td/text()').get().strip()
                        app_df.at[data_name] = self.convert_date(date_string)
                    # Non-Date
                    else:
                        app_df.at[data_name] = item.xpath('./td/text()').get().strip()
                    print(f"<{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                # Filled
                else:
                    print(f"<{item_name}> filled.") if PRINT else None
            # New (Non-Date)
            except KeyError:
                app_df[data_name] = item.xpath('./td/text()').get().strip()
                print(f"<{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None

        url = app_df.at['url'].replace('summary', 'details')
        yield SeleniumRequest(url=url, callback=self.parse_details_item, meta={'app_df': app_df})

    def parse_details_item(self, response):
        app_df = response.meta['app_df']
        items = response.xpath('//*[@id="applicationDetails"]/tbody/tr')
        n_items = len(items)
        print(f"\nFurther Information Tab: {n_items}") if PRINT else None # print(f"Further Information Tab: {n_items}")
        for item in items:
            item_name = item.xpath('./th/text()').get().strip()
            # Duplicate
            if item_name == 'Decision':
                continue
            data_name = self.details_dict[item_name]

            # if data_name in self.app_dfs.columns:
            try:
                # See source
                if item_name in ['Agent Name', 'Applicant Name', 'Case Officer']:
                    app_df.at[data_name] = item.xpath('./td/text()').get().strip()
                    print(f"<{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                # Empty
                elif self.is_empty(app_df.at[data_name]):
                    app_df.at[data_name] = item.xpath('./td/text()').get().strip()
                    print(f"<{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                # Filled
                else:
                    print(f"<{item_name}> filled.") if PRINT else None
            # New
            except KeyError:
                # if item_name in ['Actual Decision Level', 'Expected Decision Level', 'Environmental Assessment Requested']:
                app_df[data_name] = item.xpath('./td/text()').get().strip()
                print(f"<{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None

        url = app_df.at['url'].replace('summary', 'dates')
        yield SeleniumRequest(url=url, callback=self.parse_dates_item, meta={'app_df': app_df})

    def parse_dates_item(self, response):
        app_df = response.meta['app_df']
        items = response.xpath('//*[@id="simpleDetailsTable"]/tbody/tr')
        n_items = len(items)
        print(f"\nImportant Dates Tab: {n_items}") if PRINT else None #print(f"Important Dates Tab: {n_items}")
        for item in items:
            item_name = item.xpath('./th/text()').get().strip()
            # Duplicate
            if item_name in ['Application Received Date', 'Application Validated Date', 'Date Application Valid', 'Application Valid Date', 'Valid Date',
                             'Decision Issued Date']:
                continue
            data_name = self.dates_dict[item_name]

            # if data_name in self.app_dfs.columns:
            try:
                # Empty
                if self.is_empty(app_df.at[data_name]):
                    date_string = item.xpath('./td/text()').get().strip()
                    app_df.at[data_name] = self.convert_date(date_string)
                    print(f"<{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                # Filled
                else:
                    print(f"<{item_name}> filled.") if PRINT else None
            # New
            except KeyError:
                # if item_name in ['Agreed Expiry Date', 'Environmental Impact Assessment Received', 'Determination Deadline', 'Temporary Permission Expiry Date']:
                date_string = item.xpath('./td/text()').get().strip()
                app_df[data_name] = self.convert_date(date_string)
                print(f"<{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None

        url = app_df.at['url'].replace('summary', 'contacts')
        yield SeleniumRequest(url=url, callback=self.parse_contacts_item, meta={'app_df': app_df})

    def parse_contacts_item(self, response):
        app_df = response.meta['app_df']
        # categories = response.xpath('//*[@id="pa"]/div[3]/div[3]/div[3]/div')
        categories = response.css('div.tabcontainer').xpath('./div')

        # setup the app storage path.
        folder_name = str(app_df.at['name'])
        if '/' in folder_name:
            folder_name = re.sub('/', '-', folder_name)
        if '*' in folder_name:
            folder_name = re.sub(r'\*', '-', folder_name)
        folder_path = f"{self.data_storage_path}{folder_name}/"
        print(folder_path) if PRINT else None
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
        upload_folder(f"{self.data_upload_path}{folder_name}/") if CLOUD_MODE else None

        if len(categories) > 0:
            contact_categories = []
            contact_names = []
            n_names = 0
            contacts = [[]]
            max_contacts = 1
            for category in categories:  # '//*[@id="pa"]/div[3]/div[3]/div[3]/div/'
                category_name = category.xpath('./h3/text()').get()
                names = category.xpath('./p')
                # print(f"names: {len(names)}")
                for i in range(len(names)):
                    contact_name = category.xpath(f'./p[{i+1}]/text()').get()
                    if contact_name is None:  # Some portals have bugs on contact tab. We need to fix the bugs.
                        continue
                    contact_categories.append(category_name)
                    contact_names.append(contact_name)
                    n_names += 1

                    contact_details = category.xpath(f'./table[{i+1}]/tbody/tr')
                    if contact_details is None:  # Has a contact name but no contact details.
                        for j in range(max_contacts):
                            contacts[j].append('')
                    else:  # Has contact details.
                        for j, contact_detail in enumerate(contact_details):
                            contact_method = contact_detail.xpath(f'./th/text()').get()
                            contact_content = contact_detail.xpath(f'./td/text()').get()
                            try:
                                contacts[j].append(f'{contact_method}: {contact_content}')
                            except IndexError:
                                new_contact = ([''] * (n_names - 1))
                                new_contact.append(f'{contact_method}: {contact_content}')
                                contacts.append(new_contact)
                                max_contacts += 1
                        for j in range(len(contact_details), max_contacts):
                            contacts[j].append('')
            print(f"max number of contact details: {max_contacts}.") if PRINT else None
            contact_dict = {'category': contact_categories,
                            'name': contact_names}
            for i in range(max_contacts):
                contact_dict[f'contact{i+1}'] = contacts[i]
            contact_df = pd.DataFrame(contact_dict)
            contact_df.to_csv(f"{folder_path}contacts.csv", index=False)
            self.upload_and_delete(folder_name=folder_name, file_name='contacts.csv') if CLOUD_MODE else None

        # comment_url # Non_Empty
        # app_df.at['other_fields.comment_url'] = app_df.at['url'].replace('summary', 'makeComment')
        url = app_df.at['url'].replace('summary', 'neighbourComments')
        yield SeleniumRequest(url=url, callback=self.parse_public_comments_item, meta={'app_df': app_df, 'folder_name': folder_name})

    # Other Tabs: 6 + 1 {n_comments, n_constraints, n_documents, *constraint_url, docs_url, map_url, UPRN}
    def scrape_comments(self, comments, comment_source, comment_date, comment_content):
        #comments = response.xpath('//*[@id="comments"]').xpath('./div')
        def scrape_comment_source(label_name):  # scrape all texts in tag and its sub-tags.
            temp_source = comment.xpath(f'./{label_name}/text()').get().strip()
            for subtag in comment.xpath(f'./{label_name}/*'):
                temp_source += subtag.xpath('./text()').get().strip()
            return temp_source

        for comment in comments:
            try:  # h2 or h3
                if comment.xpath('./h2').get():
                    temp_source = scrape_comment_source('h2')
                elif comment.xpath('./h3').get():  # https://eplanning.northlanarkshire.gov.uk/online-applications/applicationDetails.do?activeTab=neighbourComments&keyVal=0200095FUL&neighbourCommentsPager.page=1
                    temp_source = scrape_comment_source('h3')
                else:
                    temp_source = ''
            except AttributeError:
                temp_source = ''

            comment_wraps = comment.xpath('./div')
            if len(comment_wraps) == 0:  # https://boppa.poole.gov.uk/online-applications/applicationDetails.do?keyVal=_POOLE_DCAPR_248994&activeTab=summary
                comment_source.append(temp_source)
                comment_date.append('')
                comment_content.append('')
            else:
                for comment_wrap in comment_wraps: # https://planning.n-somerset.gov.uk/online-applications/applicationDetails.do?activeTab=neighbourComments&keyVal=QMES8DLPFI100
                    comment_source.append(temp_source)
                    try:  # div/h3 or div/h4
                        temp_date = ''
                        if comment_wrap.xpath('./h3').get():
                            temp_date = comment_wrap.xpath('./h3/text()').get().strip()
                        elif comment_wrap.xpath('./h4').get():  # https://eplanning.northlanarkshire.gov.uk/online-applications/applicationDetails.do?activeTab=neighbourComments&keyVal=0200095FUL&neighbourCommentsPager.page=1
                            temp_date = comment_wrap.xpath('./h4/text()').get().strip()
                        comment_date.append(re.sub("\s+", ' ', temp_date))
                    except AttributeError:
                        temp_date = ''
                        comment_date.append('')

                    try: # div/text or div/p/text or div/div/p
                        need_date_check = True  # https://planningandwarrant.orkney.gov.uk/online-applications/applicationDetails.do?activeTab=consulteeComments&keyVal=KVXQ2HMD01600
                        temp_content = comment_wrap.xpath('./text()').getall()
                        temp_content = re.sub("\s+", ' ', ' '.join(temp_content)).strip()
                        for subtag in comment_wrap.xpath('./*'):
                            if subtag.xpath('./*/text()').get():
                                temp = subtag.xpath('./*/text()').getall()
                                temp_content += re.sub("\s+", ' ', ' '.join(temp)).strip()
                            if subtag.xpath('./text()').get():
                                if need_date_check and subtag.xpath('./text()').get().strip() == temp_date:
                                    need_date_check = False
                                    continue
                                else:
                                    temp = subtag.xpath('./text()').getall()
                                    temp_content += re.sub("\s+", ' ', ' '.join(temp)).strip()
                        comment_content.append(temp_content)
                    except AttributeError:
                        comment_content.append('')

    def parse_public_comments_item(self, response):
        time_cost = time.time() - self.start_time
        print("start scraping comments. So far time_cost: {:.0f} mins {:.4f} secs.".format(time_cost // 60, time_cost % 60))
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        # Scrape the summary of public comments
        strs = response.xpath('//*[@id="commentsContainer"]/ul/li[1]').get()
        public_consulted = int(re.search(r"\d+", strs).group())
        strs = response.xpath('//*[@id="commentsContainer"]/ul/li[2]').get()
        public_received = int(re.search(r"\d+", strs).group())

        public_consulted = max(public_consulted, public_received)
        app_df['other_fields.n_comments_public_total_consulted'] = public_consulted
        app_df['other_fields.n_comments_public_received'] = public_received

        if public_received == 0:
            app_df['other_fields.n_comments_public_objections'] = 0
            app_df['other_fields.n_comments_public_supporting'] = 0
        else:
            strs = response.xpath('//*[@id="commentsContainer"]/ul/li[3]').get()
            app_df['other_fields.n_comments_public_objections'] = int(re.search(r"\d+", strs).group())
            strs = response.xpath('//*[@id="commentsContainer"]/ul/li[4]').get()
            app_df['other_fields.n_comments_public_supporting'] = int(re.search(r"\d+", strs).group())
        print(f"\npublic comments: {public_consulted}, {public_received}, "
              f"{app_df.at['other_fields.n_comments_public_objections']}, {app_df.at['other_fields.n_comments_public_supporting']}") if PRINT else None
            #print(f"public comments: {public_consulted}, {public_received}, "
            #  f"{app_df.at['other_fields.n_comments_public_objections']}, {app_df.at['other_fields.n_comments_public_supporting']}")

        # Scrape comments
        comment_source = []
        comment_date = []
        comment_content = []
        #if public_consulted > 0:
        #    self.scrape_comments(response, comment_source, comment_date, comment_content)
        try:
            comments = response.xpath('//*[@id="comments"]').xpath('./div')
            self.scrape_comments(comments, comment_source, comment_date, comment_content)
        except TypeError:
            pass

        try:
            #next_page_url = response.xpath('//*[@id="commentsListContainer"]/p[2]/a[2]/@href').get()[0]
            next_page_url = response.xpath('//*[@id="commentsListContainer"]').css('a.next::attr(href)').get()
            #print('public:', next_page_url)
            if next_page_url:  # Next public comment page
                next_page_url = response.urljoin(next_page_url)
                yield SeleniumRequest(url=next_page_url, callback=self.parse_public_comments2_item,
                                      meta={'app_df': app_df, 'folder_name': folder_name, 'comment_source': comment_source,
                                            'comment_date': comment_date, 'comment_content': comment_content})
            else:  # Move to consultee pages
                url = app_df.at['url'].replace('summary', 'consulteeComments')
                yield SeleniumRequest(url=url, callback=self.parse_consultee_comments_item,
                                      meta={'app_df': app_df, 'folder_name': folder_name, 'comment_source': comment_source,
                                            'comment_date': comment_date, 'comment_content': comment_content})
        except TypeError:  # Public comment details can not display.
            # Move to consultee pages
            url = app_df.at['url'].replace('summary', 'consulteeComments')
            yield SeleniumRequest(url=url, callback=self.parse_consultee_comments_item,
                                  meta={'app_df': app_df, 'folder_name': folder_name, 'comment_source': comment_source,
                                        'comment_date': comment_date, 'comment_content': comment_content})

    def parse_public_comments2_item(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        comment_source = response.meta['comment_source']
        comment_date = response.meta['comment_date']
        comment_content = response.meta['comment_content']
        #self.scrape_comments(response, comment_source, comment_date, comment_content)
        try:
            comments = response.xpath('//*[@id="comments"]').xpath('./div')
            self.scrape_comments(comments, comment_source, comment_date, comment_content)
        except TypeError:
            pass

        # next_page_url = response.xpath('//*[@id="commentsListContainer"]/p[2]/a[2]/@href').get()[0]
        next_page_url = response.xpath('//*[@id="commentsListContainer"]').css('a.next::attr(href)').get()
        #print('public2:', next_page_url)
        if next_page_url:  # Next public comment page
            next_page_url = response.urljoin(next_page_url)
            yield SeleniumRequest(url=next_page_url, callback=self.parse_public_comments2_item,
                                  meta={'app_df': app_df, 'folder_name': folder_name, 'comment_source': comment_source,
                                        'comment_date': comment_date, 'comment_content': comment_content})
        else:  # Move to consultee pages
            url = app_df.at['url'].replace('summary', 'consulteeComments')
            yield SeleniumRequest(url=url, callback=self.parse_consultee_comments_item,
                                  meta={'app_df': app_df, 'folder_name': folder_name, 'comment_source': comment_source,
                                        'comment_date': comment_date, 'comment_content': comment_content})

    def parse_consultee_comments_item(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        comment_source = response.meta['comment_source']
        comment_date = response.meta['comment_date']
        comment_content = response.meta['comment_content']
        try:
            # Scrape the summary of consultee comments
            strs = response.xpath('//*[@id="commentsContainer"]/ul/li[1]').get()
            app_df['other_fields.n_comments_consultee_total_consulted'] = int(re.search(r"\d+", strs).group())
            strs = response.xpath('//*[@id="commentsContainer"]/ul/li[2]').get()
            app_df['other_fields.n_comments_consultee_responded'] = int(re.search(r"\d+", strs).group())
        except TypeError:  # Consultee page cannot display.
            app_df['other_fields.n_comments_consultee_total_consulted'] = 0
            app_df['other_fields.n_comments_consultee_responded'] = 0
        print(f"consultee comments: {app_df.at['other_fields.n_comments_consultee_total_consulted']}, {app_df.at['other_fields.n_comments_consultee_responded']}") if PRINT else None
        n_consulted_comments = app_df.at['other_fields.n_comments_consultee_total_consulted'] + app_df.at['other_fields.n_comments_public_total_consulted']
        app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_consultee_responded'] + app_df.at['other_fields.n_comments_public_received']

        # Scrape comments
        #if app_df.at['other_fields.n_comments_consultee_total_consulted'] > 0:
        #    self.scrape_comments(response, comment_source, comment_date, comment_content)
        try:
            comments = response.xpath('//*[@id="comments"]').xpath('./div')
            self.scrape_comments(comments, comment_source, comment_date, comment_content)
        except TypeError:
            pass

        # next_page_url = response.xpath('//*[@id="commentsListContainer"]/p[2]/a[2]/@href').extract()[0]
        next_page_url = response.xpath('//*[@id="commentsListContainer"]').css('a.next::attr(href)').get()
        #print('consultee:', next_page_url)
        if next_page_url:  # Next consultee comment page
            next_page_url = response.urljoin(next_page_url)
            yield SeleniumRequest(url=next_page_url, callback=self.parse_consultee_comments2_item,
                                  meta={'app_df': app_df, 'folder_name': folder_name, 'comment_source': comment_source,
                                        'comment_date': comment_date, 'comment_content': comment_content})
        else:  # Store comments and move to constraint page
            if n_consulted_comments > 0:
                comment_df = pd.DataFrame({'comment_source': comment_source,
                                           'comment_date': comment_date,
                                           'comment_content': comment_content})
                comment_df.to_csv(f"{self.data_storage_path}{folder_name}/comments.csv", index=False)
                self.upload_and_delete(folder_name=folder_name, file_name='comments.csv') if CLOUD_MODE else None
            # constraint_url  # New
            url = app_df.at['url'].replace('summary', 'constraints')
            app_df['other_fields.constraint_url'] = url
            yield SeleniumRequest(url=url, callback=self.parse_constraints_item, meta={'app_df': app_df, 'folder_name': folder_name})

    def parse_consultee_comments2_item(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        comment_source = response.meta['comment_source']
        comment_date = response.meta['comment_date']
        comment_content = response.meta['comment_content']
        #self.scrape_comments(response, comment_source, comment_date, comment_content)
        try:
            comments = response.xpath('//*[@id="comments"]').xpath('./div')
            self.scrape_comments(comments, comment_source, comment_date, comment_content)
        except TypeError:
            pass
        # for i in range(len(self.comment_source)):
        #    print(f"Comment{i+1}, source:{self.comment_source[i]},  date:{self.comment_date[i]},    content:{self.comment_content[i]}")

        # next_page_url = response.xpath('//*[@id="commentsListContainer"]/p[2]/a[2]/@href').get()[0]
        next_page_url = response.xpath('//*[@id="commentsListContainer"]').css('a.next::attr(href)').get()
        #print('consultee2:', next_page_url)
        if next_page_url:  # Next consultee comment page
            next_page_url = response.urljoin(next_page_url)
            yield SeleniumRequest(url=next_page_url, callback=self.parse_consultee_comments2_item,
                                  meta={'app_df': app_df, 'folder_name': folder_name, 'comment_source': comment_source,
                                        'comment_date': comment_date, 'comment_content': comment_content})
        else:  # Store comments and move to constraint page
            comment_df = pd.DataFrame({'comment_source': comment_source,
                                       'comment_date': comment_date,
                                       'comment_content': comment_content})
            comment_df.to_csv(f"{self.data_storage_path}{folder_name}/comments.csv", index=False)
            self.upload_and_delete(folder_name=folder_name, file_name='comments.csv') if CLOUD_MODE else None
            # constraint_url  # New
            url = app_df.at['url'].replace('summary', 'constraints')
            app_df['other_fields.constraint_url'] = url
            yield SeleniumRequest(url=url, callback=self.parse_constraints_item, meta={'app_df': app_df, 'folder_name': folder_name})

    # optional: some Idox portals do not have this tab.
    def parse_constraints_item(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        # other_fields.n_constraints
        try:
            """
            constraints_str = response.xpath('//*[@id="tab_constraints"]/span/text()').get()
            n_constraints = int(re.search(r"\d+", constraints_str).group())
            app_df.at['other_fields.n_constraints'] = n_constraints
            print(f"\nn_constraints: {n_constraints}") if PRINT else None
            if n_constraints > 0:
                tbody = response.xpath('//*[@id="caseConstraints"]/tbody')
                constraint_names = []
                constraint_types = []
                constraint_status = []
                for i in range(2, 2 + n_constraints):
                    constraint_names.append(tbody.xpath(f'./tr[{i}]/td[1]/text()').get())
                    constraint_types.append(tbody.xpath(f'./tr[{i}]/td[2]/text()').get())
                    constraint_status.append(tbody.xpath(f'./tr[{i}]/td[3]/text()').get())
                constraint_df = pd.DataFrame({'name': constraint_names,
                                              'type': constraint_types,
                                              'status': constraint_status})
                constraint_df.to_csv(f"{self.data_storage_path}{folder_name}/constraints.csv", index=False)
                self.upload_and_delete(folder_name=folder_name, file_name='constraints.csv') if CLOUD_MODE else None 
            """
            trs = response.xpath('//*[@id="caseConstraints"]/tbody/tr')[1:]
            n_constraints = int(len(trs))
            app_df.at['other_fields.n_constraints'] = n_constraints
            print(f"\nn_constraints: {n_constraints}") if PRINT else None
            if n_constraints > 0:
                constraint_names = []
                constraint_types = []
                constraint_status = []
                for tr in trs:
                    constraint_names.append(tr.xpath(f'./td[1]/text()').get())
                    constraint_types.append(tr.xpath(f'./td[2]/text()').get())
                    constraint_status.append(tr.xpath(f'./td[3]/text()').get())
                constraint_df = pd.DataFrame({'name': constraint_names,
                                              'type': constraint_types,
                                              'status': constraint_status})
                constraint_df.to_csv(f"{self.data_storage_path}{folder_name}/constraints.csv", index=False)
                self.upload_and_delete(folder_name=folder_name, file_name='constraints.csv') if CLOUD_MODE else None

        except TypeError:
            print(f"\nThis portal does not have 'Constraints' tab.") if PRINT else None #print(f"This portal does not have 'Constraints' tab.")
            app_df.at['other_fields.n_constraints'] = 0
        except AttributeError:
            print("\nNo constraints.") if PRINT else None #print("No constraints.")
            app_df.at['other_fields.n_constraints'] = 0

        # document_url
        yield SeleniumRequest(url=app_df.at['other_fields.docs_url'], callback=self.parse_documents_item, meta={'app_df': app_df, 'folder_name': folder_name})

    def unzip_documents(self, storage_path, wait_unit=1.0, wait_total=100):
        zipname = ''
        n_wait = 0

        while zipname == '' and n_wait < wait_total:
            time.sleep(wait_unit)
            n_wait += wait_unit
            rootfiles = os.listdir(get_project_root())
            for filename in rootfiles:
                # print(f"{n_wait} checking: {filename}")
                if filename.endswith('.zip'):
                    zipname = filename
                    break
        print("{:.1f} secs, zipname: {:s}".format(n_wait, zipname))

        unzip_dir = f"{storage_path}documents/"
        with zipfile.ZipFile(zipname, 'r') as zip_ref:
            zip_ref.extractall(unzip_dir)
        os.remove(zipname)
        return unzip_dir

    def get_document_info_columns(self, response):
        columns = response.xpath('//*[@id="Documents"]/tbody/tr[1]/th')
        n_columns = len(columns)
        date_column = n_columns
        type_column = n_columns
        description_column = n_columns
        for i, column in enumerate(columns):
            try:
                if 'date' in str.lower(column.xpath('./a/text()').get()):
                    date_column = i + 1
                    continue
                if 'type' in str.lower(column.xpath('./a/text()').get()):
                    type_column = i + 1
                    continue
                if 'description' in str.lower(column.xpath('./a/text()').get()):
                    description_column = i + 1
                    continue
            except TypeError:
                continue
        print(f"date column {date_column}, type column {type_column}, description column {description_column}, n_columns {n_columns}") if PRINT else None
        return date_column, type_column, description_column

    def scrape_documents_by_checkbox(self, response, driver, checkboxs, n_documents, storage_path):
        n_checkboxs = len(checkboxs)
        def rename_documents():
            docfiles = os.listdir(unzip_dir)
            docfiles.sort(key=str.lower)
            date_column, type_column, description_column = self.get_document_info_columns(response)

            # Click 'Description' button to sort documents. 点击网页上description的按钮进行排序
            description_button = None
            Descending = False
            try:
                sorting_buttons = driver.find_elements(By.CLASS_NAME, 'ascending')
                #sorting_buttons = driver.find_elements(By.XPATH, '//*[@id="Documents"]/tbody/tr')[0]
                for sorting_button in sorting_buttons:
                    if 'description' in str.lower(sorting_button.text):
                        description_button = sorting_button
                        break
                description_button.click()
            except AttributeError:
                try:
                    sorting_buttons = driver.find_elements(By.CLASS_NAME, 'descending')
                    for sorting_button in sorting_buttons:
                        if 'description' in str.lower(sorting_button.text):
                            description_button = sorting_button
                            break
                    description_button.click()
                    Descending = True
                except AttributeError:
                    print(f"failed to sort documents items.")

            # 通过driver获取排序后的文档信息
            unpaired_bases = []
            unpaired_extensions = []
            unpaired_names = []
            unpaired_identities = []
            document_items = driver.find_elements(By.XPATH, '//*[@id="Documents"]/tbody/tr')[1:]
            if Descending:
                document_items = document_items[::-1]
            print("length comparison:", len(docfiles), len(document_items)) if PRINT else None
            for i, document_item in enumerate(document_items):
                #item_info = document_item.text
                #print(item_info)
                document_date = document_item.find_element(By.XPATH, f'./td[{date_column}]').text
                document_type = document_item.find_element(By.XPATH, f'./td[{type_column}]').text
                document_description = document_item.find_element(By.XPATH, f'./td[{description_column}]').text
                item_identity = document_item.find_elements(By.XPATH, './td/a')[-1].get_attribute('href').split('-')[-1]
                item_identity = item_identity.split('.')[0]  # remove the suffix. Some doc names end with .tif but their link names end with .pdf.
                document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
                print(document_name) if PRINT else None
                if '/' in document_name:
                    document_name = re.sub('/', '-', document_name)

                docfile_base, docfile_extension = os.path.splitext(docfiles[i])
                if docfile_base.endswith(item_identity):
                    os.rename(unzip_dir + docfiles[i], f"{storage_path}{document_name}{docfile_extension}")
                else:
                    print(i+1, "- - - ", docfile_base, docfile_extension) if PRINT else None
                    unpaired_bases.append(docfile_base)
                    unpaired_extensions.append(docfile_extension)
                    unpaired_names.append(document_name)
                    unpaired_identities.append(item_identity)

            # pair item_identity with the name of downloaded documents
            for docfile_base, docfile_extension in zip(unpaired_bases, unpaired_extensions):
                print(unpaired_names) if PRINT else None
                for i, identity in enumerate(unpaired_identities):
                    if docfile_base.endswith(identity):
                        paired_name = unpaired_names[i]
                        os.rename(unzip_dir + docfile_base + docfile_extension, f"{storage_path}{paired_name}{docfile_extension}")
                        unpaired_names.remove(paired_name)
                        unpaired_identities.remove(identity)
                        continue
            os.rmdir(unzip_dir)

        max_checkboxs = 24
        n_downloads = int(np.ceil(n_checkboxs / max_checkboxs))
        n_full_downloads = n_downloads - 1
        print(f"Downloading {n_checkboxs} documents by {n_downloads} downloads ...")
        download_failure = False
        try:
            for i in range(n_full_downloads):
                start_index = i * max_checkboxs
                end_index = (i + 1) * max_checkboxs
                for checkbox in checkboxs[start_index: end_index]:
                    checkbox.click()
                time.sleep(0.1)
                driver.find_element(By.ID, 'downloadFiles').click()
                for checkbox in checkboxs[start_index: end_index]:
                    checkbox.click()
                # Unzip downloaded documents.
                unzip_dir = self.unzip_documents(storage_path)
        except FileNotFoundError as error:
            print("Downloading Failed:", error)
            download_failure = True

        try:
            start_index = n_full_downloads * max_checkboxs
            end_index = n_documents
            for checkbox in checkboxs[start_index: end_index]:
                checkbox.click()
            time.sleep(0.1)
            download_time = time.time()
            driver.find_element(By.ID, 'downloadFiles').click()
            print("Download button time cost {:.4f} secs.".format(time.time()-download_time)) if PRINT else None
            # Unzip downloaded documents.
            unzip_dir = self.unzip_documents(storage_path)
        except FileNotFoundError as error:
            print("Downloading Failed:", error)
            download_failure = True

        if download_failure:
            self.failures += 1
        else:
            rename_documents()

    # similar to the rename_documents() in scrape_documents_by_checkbox(), but without: 1>. clicking sort button 2>. pair un-matched documents.
    def rename_documents_and_get_file_urls(self, response, folder_name):
        date_column, type_column, description_column = self.get_document_info_columns(response)
        document_items = response.xpath('//*[@id="Documents"]/tbody/tr')[1:]
        document_paths = []
        file_urls = []
        for i, document_item in enumerate(document_items):
            document_date = document_item.xpath(f'./td[{date_column}]/text()').get().strip()
            document_type = document_item.xpath(f'./td[{type_column}]/text()').get().strip()
            try:
                document_description = document_item.xpath(f'./td[{description_column}]/text()').get().strip()
            except AttributeError:
                document_description = ''
            file_url = document_item.xpath('./td/a')[-1].xpath('./@href').get()
            # file_url = document_item.css('a::attr(href)').get()
            """ # the docs downloaded by file links are different from the docs downloaded from download button (.zip). Set extensions could results in crashed docs.
            try:
                item_identity = document_item.xpath('./td/input')[0].xpath('./@value').get().strip().split('-')[-1]
                print(i, item_identity)
            except TypeError:
                item_identity = file_url.split('-')[-1]
            """
            item_identity = file_url.split('-')[-1]
            document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
            print(document_name) if PRINT else None
            invalid_chars = ['/', ' ', ':']
            for invalid_char in invalid_chars:
                if invalid_char in document_name:
                    document_name = re.sub(invalid_char, '_', document_name)
            document_paths.append(f"{self.data_upload_path}{folder_name}/{document_name}")
            file_urls.append(response.urljoin(file_url))
        return document_paths, file_urls

    def scrape_documents_by_NEC(self, response, n_documents, storage_path):
        driver = response.request.meta["driver"]
        select = Select(driver.find_element(By.NAME, 'searchResult_length'))
        select.select_by_visible_text('100')
        print(f"Downloading {n_documents} documents separately ...")

        checkboxs = driver.find_elements(By.NAME, 'selectCheckBox')
        document_items = driver.find_elements(By.XPATH, '//*[@id="searchResult"]/tbody/tr')
        unzip_dir = None
        existing_names = []
        for i, checkbox in enumerate(checkboxs):
            checkbox.click()
            driver.find_element(By.ID, 'linkDownload').click()
            try:
                unzip_dir = self.unzip_documents(storage_path, wait_unit=0.1, wait_total=10)
                docfile = os.listdir(unzip_dir)[0]

                document_date = document_items[i].find_element(By.XPATH, f'./td[8]').text
                #document_date = re.sub('/', '-', document_date)
                document_type = document_items[i].find_element(By.XPATH, f'./td[3]').text
                document_description = document_items[i].find_element(By.XPATH, f'./td[7]').text
                document_filetype = document_items[i].find_element(By.XPATH, f'./td[12]').text
                document_name = f"date={document_date}&type={document_type}({document_filetype[1:].lower()})&desc={document_description}"
                if '/' in document_name:
                    document_name = re.sub('/', '-', document_name)
                if document_name not in existing_names:
                    existing_names.append(document_name)
                else:
                    rename_index = 2
                    base = document_name
                    duplicate = True
                    while duplicate:
                        document_name = base + str(rename_index)
                        if document_name not in existing_names:
                            duplicate = False
                            existing_names.append(document_name)
                        else:
                            rename_index += 1
                print(document_name) if PRINT else None
                docfile_extension = docfile.split('.')[-1]
                os.rename(unzip_dir + docfile, f"{storage_path}{document_name}.{docfile_extension.lower()}")
            except FileNotFoundError as error:
                print(f"Downloading {i}/{n_documents} Failed: {error}")
                self.failures += 1
            checkbox.click()
        if unzip_dir is not None:
            os.rmdir(unzip_dir)

    def scrape_documents_by_NEC2_USELESS(self, response, n_documents, storage_path):
        driver = response.request.meta["driver"]
        driver.find_element(By.ID, 'selectAll').click()
        driver.find_element(By.ID, 'linkDownload').click()
        print(f"Downloading {n_documents} documents by 1 download ...")
        # process zip
        try:
            unzip_dir = self.unzip_documents(storage_path)
            # Rename downloaded documents:
            docfiles = os.listdir(unzip_dir)

            driver = response.request.meta["driver"]
            select = Select(driver.find_element(By.NAME, 'searchResult_length'))
            select.select_by_visible_text('100')
            # driver.refresh()
            # time.sleep(5)

            document_items = driver.find_elements(By.XPATH, '//*[@id="searchResult"]/tbody/tr')
            # 'Correspondence for discharge of condition 53530 details of methodology and painting scheme - acceptable 17/02/2011 0 0 1.0.0 .msg'
            day = '[0-9]{2}'
            month = '\w*'
            year = '[0-9]{4}'
            pattern_date = f'{day}/{month}/{year}'
            existing_names = []

            for i, document_item in enumerate(document_items):
                document_info = document_item.text
                date_received = re.search(pattern_date, document_info, re.I).group()
                document_info = document_info.split(date_received)[0]
                date_received = re.sub('/', '-', date_received)

                case_number = re.search('\d+', document_info, re.I).group()
                document_info = document_info.split(case_number)
                document_type = document_info[0].strip()
                description = document_info[1].strip()
                docname = f"date={date_received}&type={document_type}&case_num={case_number}&desc={description}"
                if docname not in existing_names:
                    existing_names.append(docname)
                else:
                    rename_index = 2
                    base = docname
                    duplicate = True
                    while duplicate:
                        docname = base + str(rename_index)
                        if docname not in existing_names:
                            duplicate = False
                            existing_names.append(docname)
                        else:
                            rename_index += 1
                print(i, unzip_dir + docfiles[i], f"{storage_path}{docname+docfiles[i][-4:]}") if PRINT else None
                os.rename(unzip_dir + docfiles[i], f"{storage_path}{docname+docfiles[i][-4:]}")
                """
                date_received = document_item.xpath(f'./td[8]/text()').get()
                date_received = re.sub('/', '-', date_received)
                document_type = document_item.xpath(f'./td[3]/text()').get()
                case_number = document_item.xpath(f'./td[5]/text()').get()
                description = document_item.xpath(f'./td[7]/text()').get()
                docname = f"date={date_received}&type={document_type}&case_num={case_number}&desc={description}"
                os.rename(unzip_dir + docfiles[i], f"{storage_path}{docname+docfiles[-4:]}")
                #"""

            # n_remaining_documents = n_documents - 10
            # if n_remaining_documents > 0:
            #    docfiles = docfiles[10:]
            # """
            os.rmdir(unzip_dir)
        except FileNotFoundError as error:
            print("Downloading Failed:", error)
            self.failures += 1

    def parse_documents_item(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        try:
            mode_str = response.request.url.split('activeTab=')[1]
            mode = mode_str.split('&')[0]
        except IndexError as error:
            mode = 'associatedDocuments'

        if mode == 'documents':
            documents_str = response.xpath('//*[@id="tab_documents"]/span/text()').get()
            if documents_str is None:
                documents_str = response.xpath('//*[@id="pa"]/div[3]/div[3]/ul/li[3]/span/text()').get()
            if documents_str is None:
                n_documents = 0
                print(f"{app_df.name} <doc mode> n_documents: {n_documents}. (None)")
            else:
                try:
                    n_documents = int(re.search(r"\d+", documents_str).group())
                except AttributeError:
                    n_documents = len(response.xpath('//*[@id="Documents"]/tbody/tr')[1:])
                time_cost = time.time() - self.start_time
                #print(f"<doc mode> n_documents: {n_documents}")
                print(f"{app_df.name} <doc mode> n_documents: {n_documents}, folder_name: {folder_name}",
                      " time_cost: {:.0f} mins {:.4f} secs.".format(time_cost // 60, time_cost % 60))
            # other_fields.n_documents
            app_df.at['other_fields.n_documents'] = n_documents
            if n_documents > 0:
                driver = response.request.meta["driver"]
                checkboxs = driver.find_elements(By.NAME, 'file')
                if len(checkboxs) < 0:  # Download through checkboxs 24-02-17, ***Discarded. To use this approach, set 'len(checkboxs) > 0'.
                    self.scrape_documents_by_checkbox(response, driver, checkboxs, n_documents, storage_path)
                else:  # No checkboxs and the download button.
                    document_names, file_urls = self.rename_documents_and_get_file_urls(response, folder_name)
                    if not os.path.exists(self.failed_downloads_path + folder_name):
                        os.mkdir(self.failed_downloads_path + folder_name)

                    item = DownloadFilesItem()
                    item['file_urls'] = file_urls
                    item['document_names'] = document_names
                    """
                    csrf = response.xpath('//*[@id="caseDownloadForm"]/input[1]/@value').get()
                    print("csrf:", csrf)
                    item['session_csrf'] = csrf
                    """
                    cookies = driver.get_cookies()
                    print("cookies:", cookies) if PRINT else None
                    item['session_cookies'] = cookies
                    yield item
        elif mode == 'externalDocuments':
            # self.scrape_external_documents(response, app_df, storage_path)
            docs_url = response.xpath('//*[@id="pa"]/div[3]/div[3]/div[3]/p/a/@href').get()
            app_df.at['other_fields.docs_url'] = docs_url
            print(f'<{mode}> external document link:', docs_url)
            yield SeleniumRequest(url=docs_url, callback=self.parse_documents_item, meta={'app_df': app_df, 'folder_name': folder_name})
            return
        elif mode == 'associatedDocuments':
            mode_str = response.request.url.split('?')[1]
            mode_str = mode_str.split('=')[0]
            if mode_str == 'SDescription':
                system_name = 'Civica'
            elif mode_str == 'SEARCH_TYPE':  # 'FileSystemId':
                system_name = 'NEC'
            else:
                system_name = 'Unknown'
                print('Unknown document system.')

            if system_name == 'NEC':  # Bury
                documents_str = response.xpath('//*[@id="searchResult_info"]/text()').get()
                documents_str = documents_str.split('of')[1]
                n_documents = int(re.search(r"\d+", documents_str).group())
                print(f"<NEC mode> n_documents: {n_documents}")
                app_df.at['other_fields.n_documents'] = n_documents
                if n_documents > 0:
                    self.scrape_documents_by_NEC(response, n_documents, storage_path)
            if system_name == 'Civica':  # Ryedale
                pass

        else:
            print('Unknown document mode.')

        if self.is_empty(app_df.at['other_fields.uprn']):
            url = app_df.at['url'].replace('summary', 'relatedCases')
            yield SeleniumRequest(url=url, callback=self.parse_uprn_item, meta={'app_df': app_df})
        else:
            # map_url
            self.ending(app_df)
            """
            url = app_df.at['url'].replace('summary', 'map')
            app_df.at['other_fields.map_url'] = url
            yield SeleniumRequest(url=url, callback=self.parse_map_item, meta={'app_df': app_df})
            """

    def parse_uprn_item(self, response):
        app_df = response.meta['app_df']
        try:
            n_properties = response.xpath('//*[@id="Property"]/h2/span/text()').get()
            if n_properties is None:
                n_properties = response.xpath('//*[@id="Property"]/h3/span/text()').get()
            n_properties = int(re.search(r"\d+", n_properties).group())
            print(f"n_properties in Related Cases: {n_properties}") if PRINT else None
        except TypeError:  # No related case or property
            n_properties = 0

        if n_properties > 0:
            if n_properties == 1:
                url = response.xpath('//*[@id="Property"]/ul/li/a/@href').get()
            else:  # n_properties > 1
                properties = response.xpath('//*[@id="Property"]/ul/li')
                property_names = []
                for property_item in properties:
                    property_names.append(property_item.xpath('./a/text()').get().strip())
                #print(property_names)
                try:
                    matched_property = difflib.get_close_matches(app_df.at['address'], property_names, n=1)[0]
                    matched_index = property_names.index(matched_property)
                    #print(matched_index, matched_property)
                    url = response.xpath(f'//*[@id="Property"]/ul/li[{matched_index+1}]/a/@href').get()
                except IndexError as error:
                    url = None
            if url is not None:
                url = response.urljoin(url)
                print("UPRN url:", url) if PRINT else None
                yield SeleniumRequest(url=url, callback=self.parse_uprn_property_item, meta={'app_df': app_df})
            else:
                print("No UPRN.") if PRINT else None
                # map_url
                self.ending(app_df)
                """
                url = app_df.at['url'].replace('summary', 'map')
                app_df.at['other_fields.map_url'] = url
                yield SeleniumRequest(url=url, callback=self.parse_map_item, meta={'app_df': app_df})
                """
        else:  # n_properties == 0
            print("No UPRN.") if PRINT else None
            # map_url
            self.ending(app_df)
            """
            url = app_df.at['url'].replace('summary', 'map')
            app_df.at['other_fields.map_url'] = url
            yield SeleniumRequest(url=url, callback=self.parse_map_item, meta={'app_df': app_df})
            """

    def parse_uprn_property_item(self, response):
        app_df = response.meta['app_df']
        app_df.at['other_fields.uprn'] = response.xpath('//*[@id="propertyAddress"]/tbody/tr[1]/td/text()').get()
        print(f"<UPRN> scraped: {app_df.at['other_fields.uprn']}") if PRINT else None
        # map_url
        self.ending(app_df)
        """
        url = app_df.at['url'].replace('summary', 'map')
        app_df.at['other_fields.map_url'] = url
        yield SeleniumRequest(url=url, callback=self.parse_map_item, meta={'app_df': app_df})
        """

    #def parse_map_item(self, response):
        #print("it is parse_map_item")

    def ending(self, app_df):
        # Derivative data: 11
        # postcode (from address), associated_id
        # app_size, app_state, app_type, start_date, decided_date, consulted_date, reference
        # other_fields.n_dwellings, other_fields.n_statutory_days
        print("ending of an application ...") if PRINT else None
        #app_df = response.meta['app_df']
        app_df2 = pd.DataFrame(app_df).T

        folder_name = str(app_df.at['name'])
        if '/' in folder_name:
            folder_name = re.sub('/', '-', folder_name)
        if '*' in folder_name:
            folder_name = re.sub(r'\*', '-', folder_name)
        app_df2.to_csv(f"{self.result_storage_path}{app_df.name}-{folder_name}.csv", index=False)
        if CLOUD_MODE and app_df.at['other_fields.n_documents'] == 0:
            folder_path = f"{self.data_storage_path}{folder_name}"
            if not os.listdir(folder_path):
                os.rmdir(folder_path)

        time_cost = time.time() - self.start_time
        print("{:d} time_cost: {:.0f} mins {:.4f} secs.".format(app_df.name, time_cost // 60, time_cost % 60))
        if not DEVELOPMENT_MODE:
            self.to_scrape.drop(app_df.name, inplace=True)
            self.to_scrape.to_csv(self.list_path, index=True)

        # Unknown: 8
        # other_fields.appeal_date:
        # other_fields.appeal_decision_date:
        # other_fields.appeal_reference
        # other_fields.appeal_type
        # other_fields.applicant_company
        # other_fields.development_type
        # other_fields.first_advertised_date
        # other_fields.id_type


        # other_fields.comment_date
        # other_fields.decided_by: "Further Information, Actual Decision Level or Further Information, Expected Decision Level"

    def parse_item_reCaptcha(self, response):
        # bypass reCaptcha
        """
        # get token
        anchorr_url = response.css('iframe::attr(src)').extract_first()
        print(anchorr_url)
        reload_url = 'https://www.google.com/recaptcha/api2/reload?k=6LdT06weAAAAAKs4g6QtDnk3bus_DX2Vhu3yKnfi'
        token = bypass_reCaptcha(anchorr_url, reload_url)

        # find the response textarea
        driver = response.request.meta["driver"]
        reCaptcha = driver.find_element(By.ID, 'g-recaptcha-response-100000')

        # set textarea value = token
        #reCaptcha.innerHTML = token
        #reCaptcha.value = token
        #reCaptcha.send_keys(token)

        #script = "document.getElementById('g-recaptcha-response-100000').value = 'Your text goes here';"
        script = "document.getElementById('g-recaptcha-response-100000').value = '{:s}';".format(token)
        print(script)
        #script = "document.getElementById('g-recaptcha-response-100000').innerHTML = {:s};".format(token)
        driver.execute_script(script)
        print('set token.')
        time.sleep(random.uniform(20.5, 21.5))
        #"""

        file_url = response.css('.recaptcha-link::attr(href)').get()
        # file_url = response.css('html').get()

        print("***************************test:", file_url)
        file_url = response.urljoin(file_url)
        print("***************************test:", file_url)
        file_urls = [file_url]
        item = DownloadFilesItem()
        item['file_urls'] = file_urls
        yield item

