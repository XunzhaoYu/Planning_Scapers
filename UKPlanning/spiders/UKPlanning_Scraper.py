import scrapy
from scrapy_selenium import SeleniumRequest
from scrapy import signals
from scrapy import Request
#from scrapy.spiders import CrawlSpider, Rule
#from scrapy.linkextractors import LinkExtractor

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from items import DownloadFilesItem
from settings import PRINT, CLOUD_MODE, DEVELOPMENT_MODE
from spiders.document_utils import replace_invalid_characters, get_documents, get_Civica_documents, get_NEC_or_Northgate_documents, get_Exeter_documents  #, get_Northgate_documents
from tools.utils import get_project_root, get_list_storage_path, get_data_storage_path, get_filenames, Month_Eng_to_Digit, get_scraper_by_type
from tools.curl import upload_file, upload_folder
#from tools.bypass_reCaptcha import bypass_reCaptcha
#from tools.email_sender import send_emails

#import requests
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
#import csv
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
    dates_dict = {'Application Received Date':  'other_fields.date_received',  # Duplicated in summary
                  'Application Validated Date': 'other_fields.date_validated',  # Duplicated in summary
                  'Date Application Valid':     'other_fields.date_validated',  # Duplicated in summary [NewcastleUnderLyme]
                  'Application Valid Date':     'other_fields.date_validated',  # Duplicated in summary [Oadby]
                  'Valid Date':                 'other_fields.date_validated',  # New Duplicated in summary [EastHampshire]
                  'Application Registered Date': 'other_fields.date_validated',  # New Duplicated in summary [Hammersmith]

                  'Expiry Date':                    'other_fields.application_expires_date',
                  'Application Expiry Date':        'other_fields.application_expires_date',  # New Duplicate [MiltonKeynes]
                  'Application Expiry Deadline' :   'other_fields.application_expires_date',  # New Duplicate [Sefton]

                  'Statutory Expiry Date':          'other_fields.statutory_expires_date',  # New []
                  #
                  'Expiry Date for Comment':        'other_fields.comment_expires_date',  # New
                  'Expiry Date for Comments':       'other_fields.comment_expires_date',  # New Duplicate [Moray]
                  'Last Date For Comments':         'other_fields.comment_expires_date',  # New Duplicate [Edinburgh]
                  'Last Date for Comments':         'other_fields.comment_expires_date',  # New Duplicate [Glasgow]
                  'Last date for public comments':  'other_fields.comment_expires_date',  # New Duplicate [Perth]
                  'Comments To Be Submitted By':    'other_fields.comment_expires_date',  # New Duplicate [Leeds]
                  'Closing Date for Comments':      'other_fields.comment_expires_date',  # New Duplicate [Hammersmith]
                  #
                  'Actual Committee Date':          'other_fields.meeting_date',
                  'Committee Date':                 'other_fields.meeting_date',  # New Duplicate [Chelmsford]
                  'Actual Committee or Panel Date': 'other_fields.meeting_date',  # New Duplicate [Gedling]
                  'Date of Committee Meeting':      'other_fields.meeting_date',  # New Duplicate [IOW]
                  'Committee/Delegated List Date':  'other_fields.meeting_date',  # New Duplicate [WestLothian]
                  # Neighbour Consultation Date
                  'Latest Neighbour Consultation Date': 'other_fields.neighbour_consultation_start_date',
                  'Neighbours Last Notified':           'other_fields.neighbour_last_notified_date', # New [NewcastleUnderLyme]
                  'Last Date for Neighbours Responses': 'other_fields.last_neighbour_responses_date',  # New [NewcastleUnderLyme]
                  # Neighbour Consultation Expiry
                  'Neighbour Consultation Expiry Date':             'other_fields.neighbour_consultation_end_date',
                  'Neighbour Comments should be submitted by Date': 'other_fields.neighbour_consultation_end_date',  # New Duplicate [Bedford]
                  'Neighbour Notification Expiry Date':             'other_fields.neighbour_notification_expiry_date',  # New [Sefton]
                  # Consultee Consultation Date
                  'Latest Statutory Consultee Consultation Date':   'other_fields.latest_consultee_consultation_date',  # New [Bedford]
                  'Statutory Consultee Consultation Expiry Date':   'other_fields.consultee_consultation_expiry_date',  # New [Bedford]
                  # Consultation Expiry
                  'Standard Consultation Date':             'other_fields.standard_consultation_start_date',# *** changed from consultation_start to standard_cosultation_start
                  'Standard Consultation Expiry Date':      'other_fields.standard_consultation_end_date',  # *** changed from consultation_end to standard_cosultation_end

                  'Consultation Expiry Date':               'other_fields.consultation_end_date',  # New Duplicate [Chelmsford]
                  'Consultation Deadline':                  'other_fields.consultation_end_date',  # New Duplicate [NorthSomerest]
                  'Consultation Period To End On':          'other_fields.consultation_end_date',  # New Duplicate [Torbay]
                  'Consultation End Date':                  'other_fields.consultation_end_date',  # New Duplicate [TowerHamlets]

                  'Public Consultation Expiry Date':        'other_fields.public_consultation_end_date',  # New Duplicate [Oadby*** changed from consultation_end to public_xxx]
                  'Public Consultation End Date':           'other_fields.public_consultation_end_date',  # New Duplicate [IOW]
                  'Public Consultation Ends':               'other_fields.public_consultation_end_date',  # New Duplicate [Teignbridge]

                  'Overall Consultation Expiry Date':       'other_fields.overall_consultation_expires_date',  # New []
                  'Overall Date of Consultation Expiry':    'other_fields.overall_consultation_expires_date',  # New Duplicate []
                  # Advertisement
                  'Last Advertised In Press Date':      'other_fields.last_advertised_date',
                  'Advertised in Press Date':           'other_fields.last_advertised_date', # New Duplicate [Glasgow]
                  'Latest Advertisement Expiry Date':   'other_fields.latest_advertisement_expiry_date',
                  'Advertisement Expiry Date':          'other_fields.latest_advertisement_expiry_date',  # New Duplicate [NorthHertfordshire]
                  # Site Notice
                  'Last Site Notice Posted Date':   'other_fields.site_notice_start_date',
                  'Latest Site Notice Expiry Date': 'other_fields.site_notice_end_date',
                  'Site Notice Expiry Date':        'other_fields.site_notice_end_date', # New Duplicate [NorthHertfordshire]
                  # Target Date
                  'Internal Target Date':       'other_fields.target_decision_date',
                  'Target Date':                'other_fields.target_decision_date', # New Duplicate [Bedford]
                  'Target Date for Decision':   'other_fields.target_decision_date', # New Duplicate [Glasgow]
                  'Target Decision Date':       'other_fields.target_decision_date', # New Duplicate [Stroud]

                  'Revised Target Date for Decision':   'other_fields.revised_target_decision_date', # New [Glasgow]
                  'Revised Target Decision Date':       'other_fields.revised_target_decision_date',  # New Duplicate [Stroud]

                  'Agreed Extended Target Date':        'other_fields.agreed_extended_target_date',  # New [Teignbridge]
                  'Agreed Extended Date for Decision':  'other_fields.agreed_extended_decision_date', # New [IOW]
                  # Decision Date
                  'Decision Made Date':     'other_fields.decision_date',
                  'Decision Date':          'other_fields.decision_date',  # Duplicated [Hammersmith]
                  'Decision Issued Date':   'other_fields.decision_issued_date',  # Duplicated in summary

                  'Decision Notice Date':       'other_fields.decision_notice_date',  # New [NewcastleUnderLyme]
                  'Statutory Decision Date':    'other_fields.statutory_decision_date',  # New [IOW]
                  'Earliest Decision Date':     'other_fields.earliest_decision_date',  # New [NewcastleUnderLyme]
                  'Agreed Expiry Date':         'other_fields.agreed_expires_date',  # New
                  'Permission Expiry Date':     'other_fields.permission_expires_date',

                  'Decision Printed Date': 'other_fields.decision_published_date',
                  'Decision Due Date': 'other_fields.decision_due_date',  # New [Chelmsford]
                  'Environmental Impact Assessment Received': 'other_fields.environmental_assessment_date',  # New
                  # Determination
                  'Determination Deadline': 'other_fields.determination_date',  # New
                  'Statutory Determination Deadline':   'other_fields.statutory_determination_deadline',  # New []
                  'Statutory Determination Date':       'other_fields.statutory_determination_deadline',  # New Duplicate [Oadby]
                  'Statutory Determination Deadline (Unless there is an Agreed extension date above)': 'other_fields.statutory_determination_deadline', # New Duplicate [Bedford]
                  'Extended Determination Deadline': 'other_fields.extended_determination_deadline',  # New [NorthSomerest]
                  'Agreed Extension to Statutory Determination Deadline': 'other_fields.extended_determination_deadline', # New Duplicate [Bedford]

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

        def initialize_paths(data_storage_path, auth, year):
            # add auth_name
            data_storage_path = f"{data_storage_path}{auth}/"
            data_upload_path = f"{auth}/"
            if not os.path.exists(data_storage_path):
                os.mkdir(data_storage_path)
                upload_folder(data_upload_path) if CLOUD_MODE else None
            # add year
            data_storage_path = f"{data_storage_path}{year}/"
            data_upload_path = f"{data_upload_path}{year}/"
            if not os.path.exists(data_storage_path):
                os.mkdir(data_storage_path)
                upload_folder(data_upload_path) if CLOUD_MODE else None

            return data_storage_path, data_upload_path
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
            self.auth_index = 81 # 155 # 59, 239
            # Civica[13]: 4, 41, 86, 117, 123,     155(*version 2006), 168(*inaccessible), 171, 177, 192,     198, 202, 242,
            # NEC[9]: 2, 31, 60, 95(*page load issue), 113,      115, 147, 170, 182(*download failed)
            # Northgate[7]: 105[no comment], 106, 108(2003), 129, 133,     135, 143,
            # No comment[7 + 4IPs]: 87, 119(no public comment), 124, 131[IP], 181, 183[IP], 187, 228, 230[IP], 243[IP], 244
            # Dict[10]: 11(*new doc system), 88, 96[IP], 110, 140[*Northgate 2009],      209, 216, 226(*recaptcha for docs), 237, 238
            # 32+6

            # IP rotations: 5, 7, 33, 35, 43;   51, 59*[8 times], 67, 77, 104;   118, 121, 131[no comment], 142, 153;
            # 161, 178, 183[no comment], 184, 186;   190, 200, 218, 223, 225;   230[no comment], 243[no comment],
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
            self.data_storage_path, self.data_upload_path = initialize_paths(self.data_storage_path, self.auth, self.year)

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
                for filename in filenames[5:]: # https://pad-planning.bury.gov.uk/AniteIM.WebSearch/ExternalEntryPoint.aspx?SEARCH_TYPE=1&DOC_CLASS_CODE=DC&FOLDER1_REF=54751
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
            self.data_storage_path, self.data_upload_path = initialize_paths(self.data_storage_path, self.auth, self.year)

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
        self.previous_key_value = None
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
        #url = "http://lumtest.com/myip.json"
        url = "https://portal360.argyll-bute.gov.uk/planning/planning-documents?SDescription=04/00001/DET"
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
            
            pattern = r'(keyVal|NUMBER|Number|caseno|Refval|AltRef|id|theApnID|PKID)=([^&]+)'
            key_value = re.search(pattern, url).group(2)
            print(f"\n keyValue: {key_value}")
            while key_value == self.previous_key_value:
                self.index += 1
                app_df = self.app_dfs.iloc[self.index, :]
                url = app_df.at['url']
                print(f"\n Same key value:{key_value} and {self.previous_key_value} \n{app_df.name}, start url: {url}")
                key_value = re.search(pattern, url).group(2)

            self.previous_key_value = key_value
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
        # tab_lis = response.xpath('//*[@id="pa"]/div[3]/div[3]/ul').xpath('./li')
        # or response.xpath('//*[@id="pa"]/div[4]/div[3]/ul').xpath('./li')
        tab_lis = response.css('ul.tabs').xpath('./li')
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
    def parse_summary(self, app_df, items):
        for item in items:
            item_name = item.xpath('./th/text()').get().strip()
            data_name = self.summary_dict[item_name]

            # if data_name in self.app_dfs.columns:
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
        return app_df

    def parse_summary_item(self, response):
        ### Ensure the page content is loaded.
        try:
            driver = response.request.meta["driver"]
            loaded_items = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="simpleDetailsTable"]/tbody')))
            loaded_items = loaded_items.find_elements(By.XPATH, './tr')
            print('loaded items: ', len(loaded_items))
        except TimeoutException:
            # Planning Application details not available . e.g. auth=123, year=[21:]
            note = response.xpath('//*[@id="pageheading"]/h1/text()').get()
            print('note: ', note)
            # This application is no longer available for viewing. It may have been removed or restricted from public viewing.
            if note is not None and 'details not available' in note:
                print('*** *** *** This application is not available. *** *** ***')
                return
            else:
                print('*** *** *** NEED TO RELOAD APP PAGE. *** *** ***')
                #self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return
        #print('--- --- test --- ---')

        app_df = response.meta['app_df']
        self.get_doc_url(response, app_df)
        items = response.xpath('//*[@id="simpleDetailsTable"]/tbody/tr')
        n_items = len(items)
        print(f"\nSummary Tab: {n_items}") #if PRINT else None #print(f"Summary Tab: {n_items}")

        app_df = self.parse_summary(app_df, items)
        """
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
    #"""
        url = app_df.at['url'].replace('summary', 'details')
        yield SeleniumRequest(url=url, callback=self.parse_details_item, meta={'app_df': app_df})

    def parse_details_item(self, response):
        app_df = response.meta['app_df']
        items = response.xpath('//*[@id="applicationDetails"]/tbody/tr')
        n_items = len(items)
        if n_items == 0:
            print('--- --- --- --- --- ' + response.url + ' --- --- --- --- ---')
            yield SeleniumRequest(url=response.url, callback=self.parse_details_item, meta={'app_df': app_df}, dont_filter=True)
        else:
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
        if n_items == 0:
            print('--- --- --- --- --- ' + response.url + ' --- --- --- --- ---')
            yield SeleniumRequest(url=response.url, callback=self.parse_dates_item, meta={'app_df': app_df}, dont_filter=True)
        else:
            print(f"\nImportant Dates Tab: {n_items}") if PRINT else None #print(f"Important Dates Tab: {n_items}")
            for item in items:
                item_name = item.xpath('./th/text()').get().strip()
                # Duplicate
                if item_name in ['Application Received Date', 'Application Validated Date', 'Date Application Valid', 'Application Valid Date', 'Valid Date', 'Application Registered Date',
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
        folder_name = replace_invalid_characters(folder_name)
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

        # Scrape comments
        comment_source = []
        comment_date = []
        comment_content = []
        try:
            # Scrape the summary of public comments
            strs = response.xpath('//*[@id="commentsContainer"]/ul/li[1]/text()').get()
            public_consulted = int(re.search(r"\d+", strs).group())
            strs = response.xpath('//*[@id="commentsContainer"]/ul/li[2]/text()').get()
            public_received = int(re.search(r"\d+", strs).group())

            public_consulted = max(public_consulted, public_received)
            app_df['other_fields.n_comments_public_total_consulted'] = public_consulted
            app_df['other_fields.n_comments_public_received'] = public_received

            if public_received == 0:
                app_df['other_fields.n_comments_public_objections'] = 0
                app_df['other_fields.n_comments_public_supporting'] = 0
            else:
                strs = response.xpath('//*[@id="commentsContainer"]/ul/li[3]/text()').get()
                app_df['other_fields.n_comments_public_objections'] = int(re.search(r"\d+", strs).group())
                strs = response.xpath('//*[@id="commentsContainer"]/ul/li[4]/text()').get()
                app_df['other_fields.n_comments_public_supporting'] = int(re.search(r"\d+", strs).group())
            print(f"\npublic comments: {public_consulted}, {public_received}, "
                  f"{app_df.at['other_fields.n_comments_public_objections']}, {app_df.at['other_fields.n_comments_public_supporting']}") if PRINT else None
                #print(f"public comments: {public_consulted}, {public_received}, "
                #  f"{app_df.at['other_fields.n_comments_public_objections']}, {app_df.at['other_fields.n_comments_public_supporting']}")

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
        except TypeError:  # No comment page
            print('This application has no page for public comments.') if PRINT else None
            #app_df.at['other_fields.n_comments'] = 0

            app_df['other_fields.n_comments_public_total_consulted'] = 0
            app_df['other_fields.n_comments_public_received'] = 0
            app_df['other_fields.n_comments_public_objections'] = 0
            app_df['other_fields.n_comments_public_supporting'] = 0

            # Move to consultee pages
            url = app_df.at['url'].replace('summary', 'consulteeComments')
            yield SeleniumRequest(url=url, callback=self.parse_consultee_comments_item,
                                  meta={'app_df': app_df, 'folder_name': folder_name, 'comment_source': comment_source,
                                        'comment_date': comment_date, 'comment_content': comment_content})

            #app_df['other_fields.n_comments_consultee_total_consulted'] = 0
            #app_df['other_fields.n_comments_consultee_responded'] = 0
            # constraint_url  # New
            #url = app_df.at['url'].replace('summary', 'constraints')
            #app_df['other_fields.constraint_url'] = url
            #yield SeleniumRequest(url=url, callback=self.parse_constraints_item, meta={'app_df': app_df, 'folder_name': folder_name})

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
            strs = response.xpath('//*[@id="commentsContainer"]/ul/li[1]/text()').get()
            app_df['other_fields.n_comments_consultee_total_consulted'] = int(re.search(r"\d+", strs).group())
            strs = response.xpath('//*[@id="commentsContainer"]/ul/li[2]/text()').get()
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
            #print('This application has no consultee comments.') if PRINT else None
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


    def parse_documents_item(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        def create_item(folder_name, file_urls, document_names):
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

            driver = response.request.meta["driver"]
            cookies = driver.get_cookies()
            print("cookies:", cookies) if PRINT else None
            item['session_cookies'] = cookies
            return item

        try:
            mode_str = response.request.url.split('activeTab=')[1]
            mode = mode_str.split('&')[0]
        except IndexError as error:
            mode = 'associatedDocuments'

        if mode == 'documents':
            ### get n_documents ###
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

            ### download documents ###
            if n_documents > 0:
                #document_names, file_urls = self.rename_documents_and_get_file_urls(response, self.data_upload_path, folder_name)
                file_urls, document_names = get_documents(response, self.data_upload_path, folder_name)
                item = create_item(folder_name, file_urls, document_names)
                yield item
        elif mode == 'externalDocuments':  # Identify and redirect to the associated third-party document systems.
            docs_url = response.css('div.tabcontainer.toplevel').xpath('./p/a/@href').get()
            #docs_url = response.xpath('//*[@id="pa"]/div[3]/div[3]/div[3]/p/a/@href').get()
            # Civica:       //*[@id="pa"]/div[3]/div[3]/div[3]/p/a
            # NEC:          //*[@id="pa"]/div[3]/div[3]/div[3]/p/a
            # Northgate:    //*[@id="pa"]/div[3]/div[3]/div[9]/p/a
            app_df.at['other_fields.docs_url'] = docs_url
            print(f'<{mode}> external document link:', docs_url)
            yield SeleniumRequest(url=docs_url, callback=self.parse_documents_item, meta={'app_df': app_df, 'folder_name': folder_name})
            return
        elif mode == 'associatedDocuments':  # Scrape the associated third-party document systems.
            mode_str = response.request.url.split('?')[1]
            print('mode_str: ', mode_str) if PRINT else None
            system_name = 'Unknown'
            #if 'SDescription' in mode_str or 'ref_no' in mode_str:
            if any(x in mode_str for x in ('SDescription', 'ref_no')):
                """ Civica [13 LAs] examples:
                [4]
                [41]
                [86]
                [117]
                [123]
                https://myserviceplanning.gateshead.gov.uk/Planning/planning-documents?SDescription=DC/03/01849/FUL
                [155 Norwich| SDescription |Civica 2006] https://documents.norwich.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=06/00022/F
                [***168 ]
                [171]
                [177]
                [192]
                [198]
                [202]
                [242]
                https://documents.richmondshire.gov.uk/planning/planning-documents?SDescription=03/01572/LBC&viewdocs=true
                https://padocs.lewes-eastbourne.gov.uk/planning/planning-documents?ref_no=LW/04/0007
                """
                system_name = 'Civica'
            elif 'SEARCH_TYPE' in mode_str or 'FileSystemId' in mode_str or 'doc_class_code' in mode_str:
                """
                NEC [9 LAs]:
                [2 AdurWorthing| FileSystemId] https://docs.adur-worthing.gov.uk/PublicAccess_Live/SearchResult/RunThirdPartySearch?FileSystemId=DA&FOLDER1_REF=SU/1/01/TP/18916
                [31 Bury| SEARCH_TYPE] https://pad-planning.bury.gov.uk/AniteIM.WebSearch/ExternalEntryPoint.aspx?SEARCH_TYPE=1&DOC_CLASS_CODE=DC&FOLDER1_REF=49332
                [60 DerbyshireDales| SEARCH_TYPE] https://plandocs.derbyshiredales.gov.uk/PublicAccess_Live/ExternalEntryPoint.aspx?SEARCH_TYPE=1&DOC_CLASS_CODE=PD&FOLDER1_REF=04/01/0027
                [***95 Hambleton*** |?] 
                [113 Knowsley| FileSystemId] https://epa2.knowsley.gov.uk/PublicAccess_Live/SearchResult/RunThirdPartySearch?FileSystemId=DC&FOLDER1_REF=04/00001/FUL
                [115 Lancaster| FileSystemId] https://planningdocstest.lancaster.gov.uk/PublicAccess_Live/SearchResult/RunThirdPartySearch?Folder1_Ref=03/00007/FUL&FileSystemId=DC 
                [147 NorthHertfordshire| doc_class_code] https://documentportal.north-herts.gov.uk/GetDocList/Default.aspx?doc_class_code=DC&case_number=03/00004/1
                [170 Rhondda| SEARCH_TYPE] https://documents0122.rctcbc.gov.uk/Publicaccess_Live/ExternalEntrypoint.aspx?SEARCH_TYPE=1&DOC_CLASS_CODE=DC&folder1_ref=01/6003/10
                [***182 Selby***download error| FileSystemId] http://publicaccess1.selby.gov.uk/PublicAccess_LIVE/SearchResult/RunThirdPartySearch?FileSystemId=PL&FOLDER1_REF=CO/2004/0021
                
                Northgate [7 LAs]:
                [105 Hinckley| SEARCH_TYPE] https://publicdocuments.hinckley-bosworth.gov.uk/PublicAccess_LIVE/ExternalEntryPoint.aspx?SEARCH_TYPE=1&DOC_CLASS_CODE=PL&FOLDER1_REF=11/00935/FUL
                [106 Horsham| FileSystemId] https://iawpa.horsham.gov.uk/PublicAccess_LIVE/SearchResult/RunThirdPartySearch?FileSystemId=DH&FOLDER1_REF=DC/14/0006
                [108 Huntingdonshire| FileSystemId] https://docs.huntingdonshire.gov.uk/PublicAccess_Live/SearchResult/RunThirdPartySearch?FileSystemId=PS&FOLDER1_REF=1102106FUL
                [129 MerthyrTydfil| FileSystemId] https://enterprise.merthyr.gov.uk/PublicAccess_LIVE/SearchResult/RunThirdPartySearch?FileSystemId=DC&FOLDER1_REF=P/12/0009
                [133 MidSussex| FileSystemId] https://padocs.midsussex.gov.uk/PublicAccess_Live/SearchResult/RunThirdPartySearch?FileSystemId=DM&FOLDER1_REF=DM/22/0020
                [135 MiltonKeynes| FileSystemId] https://npaedms.milton-keynes.gov.uk/PublicAccess_LIVE/SearchResult/RunThirdPartySearch?FileSystemId=DC&FOLDER1_REF=18/00002/FUL
                [143 Newport| FileSystemId] https://documents.newport.gov.uk/PublicAccess_LIVE/SearchResult/RunThirdPartySearch?FileSystemId=PL&FOLDER1_REF=12/0004]
                """
                try:
                    powered_by = response.css('.powered-by').xpath('./a/text()').get()
                    print(powered_by)
                    if 'NEC' in powered_by:
                        system_name = 'NEC'
                    elif 'Northgate' in powered_by:
                        system_name = 'Northgate'
                except TypeError:
                    copyright = response.xpath('//*[@id="tblFooter"]/tbody/tr/td/p/text()').get()
                    if 'NEC' in copyright:
                        system_name = 'NEC'
                    elif 'Northgate' in copyright:
                        system_name = 'Northgate'
            elif 'appref' in mode_str:
                # [81] Exeter https://exeter.gov.uk/planning-services/permissions-and-applications/related-documents?appref=06/0009/FUL
                system_name = 'Exeter'

                driver = response.request.meta["driver"]
                document_tree = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CLASS_NAME, 'tree')))
                # //*[@id="main-content"]/div/div[1]/article/section/div[2]
                document_items = document_tree.find_elements(By.XPATH, '//*[@id="1"]')
                n_documents = len(document_items)
                print(f"{app_df.name} <{system_name} mode> n_documents: {n_documents}, folder_name: {folder_name}")
                app_df.at['other_fields.n_documents'] = n_documents

                ### download documents ###
                if n_documents > 0:
                    file_urls, document_names = get_Exeter_documents(response, document_tree, n_documents, self.data_upload_path, folder_name)
                    item = create_item(folder_name, file_urls, document_names)
                    yield item

            if system_name == 'Unknown':
                print('Unknown document system.')

            if system_name == 'Civica':  # Ryedale
                ### get n_documents ###
                driver = response.request.meta["driver"]
                # application_viewer = '//*[@id="applicationviewer"]'
                # civica_document_list = '//*[@id="applicationviewer"]/div/div/div[2]/div/div[3]/div'
                # civica_doclist = '//*[@id="applicationviewer"]/div/div/div[2]/div/div[3]/div/div/div/div'
                Civica_version = 2024
                try:
                    #document_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'{civica_doclist}/ul')))
                    document_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CLASS_NAME, 'civica-doclist')))
                    document_items = document_list.find_elements(By.XPATH, './ul/li')
                    #print('driver document items', document_items)
                    n_documents = len(document_items)
                except TimeoutException:
                    try:
                        document_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, '_id58:data:tbody_element')))
                        document_items = document_list.find_elements(By.XPATH, './tr')
                        n_documents = len(document_items)
                        Civica_version = 2006
                    except TimeoutException:
                        print('No documents are available.') if PRINT else None
                        n_documents = 0

                print(f"{app_df.name} <{system_name} mode (ver.{Civica_version})> n_documents: {n_documents}, folder_name: {folder_name}")
                app_df.at['other_fields.n_documents'] = n_documents

                ### download documents ###
                if n_documents > 0:
                    file_urls, document_names = get_Civica_documents(response, document_items, n_documents, self.data_upload_path, folder_name, Civica_version)
                    item = create_item(folder_name, file_urls, document_names)
                    yield item

            if system_name == 'NEC' or system_name == 'Northgate':
                # NEC: AdurWorthing, Bury
                ### get n_documents ###
                version = 2024
                try:
                    documents_str = response.xpath('//*[@id="searchResult_info"]/text()').get()  # documents_str = 'Showing 1 to 10 of {n_documents} entries'
                    documents_str = documents_str.split('of')[1]  # documents_str = '{n_documents} entries'
                    n_documents = int(re.search(r"\d+", documents_str).group())
                except AttributeError:
                    try:
                        # //*[@id="PanelMain"]/div[1]
                        documents_str = response.css('div.TitleLabel').xpath('./text()').get()  # documents_str = 'Search Results - {n_documents} records found'
                        n_documents = int(re.search(r"\d+", documents_str).group())
                        version = 2009
                    except TypeError:
                        print('No documents are available.') if PRINT else None
                        n_documents = 0

                print(f"{app_df.name} <{system_name} mode (ver.{version})> n_documents: {n_documents}, folder_name: {folder_name}")
                app_df.at['other_fields.n_documents'] = n_documents

                ### download documents ###
                if n_documents > 0:
                    file_urls, document_names = get_NEC_or_Northgate_documents(response, n_documents, self.data_upload_path, folder_name, version)
                    item = create_item(folder_name, file_urls, document_names)
                    yield item
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
        folder_name = replace_invalid_characters(folder_name)
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

