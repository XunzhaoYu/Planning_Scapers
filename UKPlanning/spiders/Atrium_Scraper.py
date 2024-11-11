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
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException, ElementClickInterceptedException
from selenium.webdriver.support.ui import Select

from items import DownloadFilesItem
from settings import PRINT, CLOUD_MODE, DEVELOPMENT_MODE
from spiders.document_utils import replace_invalid_characters, get_documents, get_Civica_documents, get_NEC_or_Northgate_documents  #, get_Northgate_documents
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

"""
 directory: '..\\..\\..\\ScrapedApplications\\Huntingdonshire\\2007\\Huntingdonshire_0702015FUL\\date=28 10 2010&type=General Document Public Access&desc=Lighting Report May 2007&filetype=.pdf&uid=151A76FCE2C811DFB8C10013CE24F10B.pdf'

No such file or directory: '..\\..\\..\\ScrapedApplications\\MerthyrTydfil\\2022\\MerthyrTydfil_P_22_0333\\date=22 12 2022&type=Approved Plan&desc=Existing and Proposed Block Plans Drawing No. 1812-01 Approved with conditions 26.1.23&uid=75EDBBE23D794DAAACBAC7BD7E1EBC93.pdf'
"""

""" 62
items = ['other_fields.applicant_company', 'other_fields.agent_company', 'other_fields.application_type', 'other_fields.id_type', 'other_fields.meeting_date', 'other_fields.district', 'other_fields.docs_url', 'other_fields.comment_date', 'other_fields.n_dwellings', 'address', 'other_fields.appeal_status', 'app_type', 'other_fields.neighbour_consultation_start_date', 'other_fields.decision_date', 'other_fields.applicant_name', 'postcode', 'other_fields.n_documents', 'reference', 'other_fields.neighbour_consultation_end_date', 'other_fields.appeal_result', 'other_fields.n_constraints', 'other_fields.n_comments', 'other_fields.decision', 'other_fields.date_received', 'associated_id', 'app_state', 'other_fields.appeal_reference', 'other_fields.status', 'other_fields.uprn', 'other_fields.latest_advertisement_expiry_date', 'start_date', 'consulted_date', 'other_fields.case_officer', 'other_fields.applicant_address', 'other_fields.planning_portal_id', 'other_fields.permission_expires_date', 'decided_date', 'other_fields.agent_name', 'other_fields.appeal_date', 'other_fields.site_notice_start_date', 'other_fields.decision_published_date', 'other_fields.parish', 'other_fields.agent_address', 'other_fields.last_advertised_date', 'other_fields.ward_name', 'other_fields.consultation_end_date', 'other_fields.consultation_start_date', 'other_fields.application_expires_date', 'other_fields.decision_issued_date', 'other_fields.n_statutory_days', 'other_fields.development_type', 'other_fields.date_validated', 'other_fields.target_decision_date', 'altid', 'other_fields.first_advertised_date', 'other_fields.map_url', 'other_fields.appeal_decision_date', 'description', 'app_size', 'other_fields.appeal_type', 'other_fields.site_notice_end_date', 'other_fields.decided_by']
y = []
remaining = [item for item in items if item not in y]
print(len(items), len(y), len(remaining))
print(remaining)
# """

class Atrium_Scraper(scrapy.Spider):
    name = 'Atrium_Scraper'

    # 81 ['location_y', 'other_fields.comment_url', 'other_fields.applicant_company', 'other_fields.agent_company', 'other_fields.application_type', 'other_fields.id_type', 'other_fields.meeting_date', 'other_fields.district', 'other_fields.docs_url', 'uid', 'other_fields.comment_date', 'other_fields.n_dwellings', 'url', 'location_x', 'other_fields.latitude', 'other_fields.northing', 'address', 'other_fields.appeal_status', 'app_type', 'other_fields.neighbour_consultation_start_date', 'other_fields.decision_date', 'area_name', 'other_fields.applicant_name', 'postcode', 'other_fields.n_documents', 'reference', 'other_fields.neighbour_consultation_end_date', 'other_fields.appeal_result', 'other_fields.n_constraints', 'last_changed', 'other_fields.n_comments', 'other_fields.decision', 'other_fields.date_received', 'associated_id', 'app_state', 'other_fields.appeal_reference', 'last_scraped', 'other_fields.status', 'other_fields.uprn', 'scraper_name', 'link', 'other_fields.latest_advertisement_expiry_date', 'start_date', 'consulted_date', 'other_fields.case_officer', 'other_fields.longitude', 'other_fields.applicant_address', 'other_fields.planning_portal_id', 'other_fields.permission_expires_date', 'last_different', 'decided_date', 'other_fields.agent_name', 'location', 'other_fields.appeal_date', 'other_fields.site_notice_start_date', 'other_fields.decision_published_date', 'other_fields.parish', 'other_fields.lng', 'other_fields.agent_address', 'other_fields.last_advertised_date', 'other_fields.ward_name', 'other_fields.consultation_end_date', 'other_fields.consultation_start_date', 'other_fields.lat', 'other_fields.application_expires_date', 'other_fields.decision_issued_date', 'other_fields.n_statutory_days', 'other_fields.development_type', 'other_fields.date_validated', 'other_fields.target_decision_date', 'altid', 'other_fields.first_advertised_date', 'other_fields.map_url', 'area_id', 'other_fields.appeal_decision_date', 'description', 'app_size', 'other_fields.appeal_type', 'other_fields.easting', 'other_fields.site_notice_end_date', 'other_fields.decided_by']
    # 72 (no locations) ['other_fields.comment_url', 'other_fields.applicant_company', 'other_fields.agent_company', 'other_fields.application_type', 'other_fields.id_type', 'other_fields.meeting_date', 'other_fields.district', 'other_fields.docs_url', 'uid', 'other_fields.comment_date', 'other_fields.n_dwellings', 'url', 'address', 'other_fields.appeal_status', 'app_type', 'other_fields.neighbour_consultation_start_date', 'other_fields.decision_date', 'area_name', 'other_fields.applicant_name', 'postcode', 'other_fields.n_documents', 'reference', 'other_fields.neighbour_consultation_end_date', 'other_fields.appeal_result', 'other_fields.n_constraints', 'last_changed', 'other_fields.n_comments', 'other_fields.decision', 'other_fields.date_received', 'associated_id', 'app_state', 'other_fields.appeal_reference', 'last_scraped', 'other_fields.status', 'other_fields.uprn', 'scraper_name', 'link', 'other_fields.latest_advertisement_expiry_date', 'start_date', 'consulted_date', 'other_fields.case_officer', 'other_fields.applicant_address', 'other_fields.planning_portal_id', 'other_fields.permission_expires_date', 'last_different', 'decided_date', 'other_fields.agent_name', 'other_fields.appeal_date', 'other_fields.site_notice_start_date', 'other_fields.decision_published_date', 'other_fields.parish', 'other_fields.agent_address', 'other_fields.last_advertised_date', 'other_fields.ward_name', 'other_fields.consultation_end_date', 'other_fields.consultation_start_date', 'other_fields.application_expires_date', 'other_fields.decision_issued_date', 'other_fields.n_statutory_days', 'other_fields.development_type', 'other_fields.date_validated', 'other_fields.target_decision_date', 'altid', 'other_fields.first_advertised_date', 'other_fields.map_url', 'area_id', 'other_fields.appeal_decision_date', 'description', 'app_size', 'other_fields.appeal_type', 'other_fields.site_notice_end_date', 'other_fields.decided_by']
    # 62 (no non_empty) ['other_fields.applicant_company', 'other_fields.agent_company', 'other_fields.application_type', 'other_fields.id_type', 'other_fields.meeting_date', 'other_fields.district', 'other_fields.docs_url', 'other_fields.comment_date', 'other_fields.n_dwellings', 'address', 'other_fields.appeal_status', 'app_type', 'other_fields.neighbour_consultation_start_date', 'other_fields.decision_date', 'other_fields.applicant_name', 'postcode', 'other_fields.n_documents', 'reference', 'other_fields.neighbour_consultation_end_date', 'other_fields.appeal_result', 'other_fields.n_constraints', 'other_fields.n_comments', 'other_fields.decision', 'other_fields.date_received', 'associated_id', 'app_state', 'other_fields.appeal_reference', 'other_fields.status', 'other_fields.uprn', 'other_fields.latest_advertisement_expiry_date', 'start_date', 'consulted_date', 'other_fields.case_officer', 'other_fields.applicant_address', 'other_fields.planning_portal_id', 'other_fields.permission_expires_date', 'decided_date', 'other_fields.agent_name', 'other_fields.appeal_date', 'other_fields.site_notice_start_date', 'other_fields.decision_published_date', 'other_fields.parish', 'other_fields.agent_address', 'other_fields.last_advertised_date', 'other_fields.ward_name', 'other_fields.consultation_end_date', 'other_fields.consultation_start_date', 'other_fields.application_expires_date', 'other_fields.decision_issued_date', 'other_fields.n_statutory_days', 'other_fields.development_type', 'other_fields.date_validated', 'other_fields.target_decision_date', 'altid', 'other_fields.first_advertised_date', 'other_fields.map_url', 'other_fields.appeal_decision_date', 'description', 'app_size', 'other_fields.appeal_type', 'other_fields.site_notice_end_date', 'other_fields.decided_by']
    Non_empty = ['uid', 'scraper_name', 'url', 'link', 'area_id', 'area_name'] + \
                ['last_scraped', 'last_different', 'last_changed', 'other_fields.comment_url']  # 10 = 6 + 4
    Locations = ['location', 'location_x', 'location_y', 'other_fields.lat', 'other_fields.latitude'] + \
                ['other_fields.lng', 'other_fields.longitude']  # 9 = 5 + 2 + 'other_fields.easting' + 'other_fields.northing'

    def __init__(self, auth_index, year):
        super().__init__()
        self.start_time = time.time()
        self.data_storage_path = get_data_storage_path()
        self.auth_index = int(auth_index)
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
        # if True:
        #    pass
        if DEVELOPMENT_MODE:
            auth_names = get_scraper_by_type('Atrium')  # Atrium
            auth_names = [auth_name for auth_name in auth_names if not auth_name.startswith('.')]
            auth_names.sort(key=str.lower)
            print(auth_names)

            app_dfs = []
            self.auth_index = 15  #1, 3, 13
            """
            # A: 0[Bridgend]    # [Details], [Other Details], [Decision], [Consultees], [Documents], [Public Notices]
            # B: 1[Cherwell]    # [Main Details], [Applicant/Agents], [Publicity], [Supporting Docs], [Properties], [Site History]
            #    26 [WestNorthamptonshire] # https://wnc.planning-register.co.uk/Planning/Display/N/2001/6
            #       (Disclaimer/ Copyright/ Personal Data - Terms and Conditions | accept)
            # C: 2 [Crawley]     # [Main Details], [Applicant], [Publicity], [Supporting Documents]
            # D: 3 [Cumbria]        # Mage Page | Multiple Document Tabs.  https://planning.cumbria.gov.uk/Planning/Display/5/01/9006#undefined
            #    12[Leicestershire] # Mage Page | Multiple Document Tabs.  https://leicestershire.planning-register.co.uk/Disclaimer?returnUrl=%2FPlanning%2FDisplay%3FapplicationNumber%3D2001%252F9200%252F03
            #       (Planning portal Disclaimer | Agree)
            # Unknown: 4[Derbyshire], 8[Hertfordshire], 18 [Oxfordshire]
            #
            # E: 5[Essex]       # https://planning.essex.gov.uk/Planning/Display/CC/COL/07/01  #***# Appeal details
            #       (Copyright & Disclaimer - Application search | Agree)
            #       [Main Details], [Location], [Map], [Associated Documents], [Consultees], [Appeal Details]
            #
            # F: 6[Fylde]F1             # https://pa.fylde.gov.uk/Planning/Display/01/0005
            #    11 [Leicester]F2       # https://planning.leicester.gov.uk/Planning/Display/20010007
            #    14 [MalvernHills]F1    # https://plan.malvernhills.gov.uk/Planning/Display/99/01062/ADV  year1999 index2 with appeal details
            #    16 [NorthDevon]        # https://planning.northdevon.gov.uk/Planning/Display/30457
            #    25* [WestmorlandFurness] # https://planningregister.westmorlandandfurness.gov.uk/Planning/Display/5010005
            #    28 [Worcester]         # https://plan.worcester.gov.uk/Planning/Display/P01L0007
            #    29 [Wychavon]          # https://plan.wychavon.gov.uk/Planning/Display/W/01/00069/PN
            #       (Disclaimer | Agree)
            #       Planning Online Status | [Application Details(Summary/Important Dates/Further Information/ Condition Details//Information Notes)], [Documents], ([Consultations]), ([Map]), [Appeals]  #***#  Application Details + Appeals
            #
            # G ~ A: 7 [Glamorgan]  # https://vogonline.planning-register.co.uk/Planning/Display/2001/00019/FUL
            #   ~ A: 19 [Redcar]    # https://planning.redcar-cleveland.gov.uk/Planning/Display?applicationNumber=R%2F2001%2F0001%2FFF
            #
            # H: 9  [Kent]      # https://www.kentplanningapplications.co.uk/Planning/Display/GR/01/53%20-%20GR/01/748/R5,R14  #***# Main Details end with 'District(s)'
            #       (Search and view planning applications – Disclaimer, Copyright Information and Privacy Statement | Continue)
            #       [Main Details], [Map]
            # H2: 20 [Somerset]  # https://planning.somerset.gov.uk/Planning/Display?applicationNumber=089639%2F010
            #       (Disclaimer | Agree) cookie.
            #
            # I: 10 [Lancashire]  # https://planningregister.lancashire.gov.uk/Planning/Display/08/02/0031  #***#  Applicants + Attachments
            #       [Main Details], [Applicants], [Consultees and Constraints], [Committee], [Attachments]
            #
            # K: 13 [Lincolnshire]  # https://lincolnshire.planning-register.co.uk/Disclaimer?returnUrl=%2FPlanning%2FDisplay%3FapplicationNumber%3DPL%255C0091%255C06
            #       (Disclaimer | Agree)
            #       [Main Details], [Associated Documents], [Consultees]  #***#
            #"""

            # L: 15 [Norfolk]  # https://eplanning.norfolk.gov.uk/Planning/Display/L/3/2001/3003  #***#  Main Details + Appeals
            #       [Main Details], [Location], [Documents], [Constraints], [Consultations], [Appeals]
            #
            # M: 17 [NorthumberlandPark]  # https://nnpa.planning-register.co.uk/Planning/Display/02NP0007 #***#  没有main details, 注意与F区分
            #    21 [SouthWestDevon]    # http://apps.westdevon.gov.uk/PlanningSearchMVC/Home/Details/021514
            #       [Proposal and Location], [Applicant Details], [Consultation], [Decision], [Documents]
            #
            # N: 22 [Suffolk]  # http://suffolk.planning-register.co.uk/Planning/Display?applicationNumber=SE%2F01%2F2648%2FP  # Information displayed in main page without tabs. (somehow similar to D)
            #       (PLANNING ONLINE REGISTER – COPYRIGHT AND DISCLAIMER | Agree)
            #       (Details) | [Other Details], [Location]
            #

            # O: 23 [Surrey]  # https://planning.surreycc.gov.uk/Planning/Display/PL1914  #***#  Applicant/Agent + Attachments
            #       accept
            #       [Main Details], [Applicant/Agent], [Consultation], [Decision], [Attachments]
            #
            # P: 24 [WelwynHatfield]  #  https://planning.welhat.gov.uk/Planning/Display/S6/2001/0028/FP
            #       [Main Details], [Constraints], [Location], [Decision], [Documents], [Consultees], [Neighbours], [History], [NMAs]  #***$ NMAs
            #
            # Q: 27 [WestSussex]  # https://westsussex.planning-register.co.uk/Planning/Display/SY/114/07
            #       [Main Details], [Map], [Associated Documents]  #***# Check if the same as E

            # 44, 73, 92, 94, 101,     135, 146, 149, 176, 194 Kent,
            # 202, 205, 206, 211, 218, 247, 249, 261, 275, 289 Radcar,
            # 322, 340, 354, 356, 392, 400, 401, 405, 415, 418

            self.year = -1
            self.auth = auth_names[int(self.auth_index)]
            self.data_storage_path, self.data_upload_path = initialize_paths(self.data_storage_path, self.auth, self.year)

            # years = np.linspace(2002, 2021, 20, dtype=int)
            # years = np.linspace(2003, 2022, 20, dtype=int)  # 11
            # years = np.append(years[:14], years[15:])
            # 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021
            sample_index = 5
            for auth in auth_names[self.auth_index:self.auth_index + 1]:  # 5, 15, 16, 18
                # for year in years:
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
            self.auth = auth_names[int(self.auth_index)]
            # option1: scrape all years
            year = int(self.year)
            if year < 0:
                src_path = f"{get_list_storage_path()}{self.auth}/"
                src_filenames = os.listdir(src_path)
                src_filenames.sort(key=str.lower)
                # src_files = []
                # for filename in src_filenames:
                #    if not filename.startswith('.'):
                #        src_files.append(src_path+filename)
                src_files = [src_path + filename for filename in src_filenames if not filename.startswith('.')]
                self.app_dfs = pd.concat([pd.read_csv(file) for file in src_files], ignore_index=True)
            # option2: scrape a given year
            else:
                src_path = f"{get_list_storage_path()}{self.auth}/{self.auth}{self.year}.csv"
                self.app_dfs = pd.read_csv(src_path)
            self.data_storage_path, self.data_upload_path = initialize_paths(self.data_storage_path, self.auth, self.year)

            # read the list of scraping.
            self.list_path = f"{self.data_storage_path}to_scrape_list.csv"
            if not os.path.isfile(self.list_path):
                self.init_index = 0  # 2173 if int(self.year)==2003 else 2132 #1004
                self.to_scrape = self.app_dfs.iloc[self.init_index:, 0]
                self.to_scrape.to_csv(self.list_path, index=True)
                print("write", self.to_scrape)
            else:
                self.to_scrape = pd.read_csv(self.list_path, index_col=0)
                if self.to_scrape.empty:
                    sys.exit("To_scrape_list is empty.")
                else:
                    print("read", self.to_scrape)
            self.app_dfs = self.app_dfs.iloc[self.to_scrape.index, :]
        print(self.app_dfs)

        # settings.
        self.index = -1
        # self.index = self.init_index
        self.failures = 0
        # self.failed_apps = []

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
                # self.handle_error_log()

        # allowed_domains = ['pa.bexley.gov.uk']
        # start_urls = ['https://pa.bexley.gov.uk/online-applications/applicationDetails.do?keyVal=LELZV9BE01D00&activeTab=summary']
        self.parse_func = self.parse_data_item_A  # [Details], [Other Details], [Decision], [Consultees], [Documents], [Public Notices]
        if auth_names[self.auth_index] in ['Cherwell', 'WestNorthamptonshire']:
            self.parse_func = self.parse_data_item_B  # [Main Details], [Applicant/Agents], [Publicity], [Supporting Docs], [Properties], [Site History]
        elif auth_names[self.auth_index] in ['Crawley']:
            self.parse_func = self.parse_data_item_C  # [Main Details], [Applicant], [Publicity], [Supporting Documents]
        elif auth_names[self.auth_index] in ['Cumbria']:  # https://planning.cumbria.gov.uk/Planning/Display/5/01/9006#undefined
            self.parse_func = self.parse_data_item_Cumbria  # Main page with two tabs: [Application Documents], [Decision]
        elif auth_names[self.auth_index] in ['Essex']:  # https://planning.essex.gov.uk/Planning/Display/CC/COL/07/01
            self.parse_func = self.parse_data_item_Essex
        elif auth_names[self.auth_index] in ['Fylde', 'MalvernHills', 'NorthDevon', 'WestmorlandFurness', 'Worcester', 'Wychavon']:
            self.parse_func = self.parse_data_item_Fylde
        elif auth_names[self.auth_index] in ['Glamorgan']:
            self.parse_func = self.parse_data_item_Glamorgan
        elif auth_names[self.auth_index] in ['Kent']:
            self.parse_func = self.parse_data_item_Kent
        elif auth_names[self.auth_index] in ['Lancashire']:
            self.parse_func = self.parse_data_item_Lancashire
        elif auth_names[self.auth_index] in ['Leicester']:
            self.parse_func = self.parse_data_item_Leicester
        elif auth_names[self.auth_index] in ['Leicestershire']:
            self.parse_func = self.parse_data_item_Leicestershire
        elif auth_names[self.auth_index] in ['Lincolnshire']:
            self.parse_func = self.parse_data_item_Lincolnshire
        elif auth_names[self.auth_index] in ['Redcar']:
            self.parse_func = self.parse_data_item_Redcar
        elif auth_names[self.auth_index] in ['Somerset']:
            self.parse_func = self.parse_data_item_Somerset
        else:
            pass

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(Atrium_Scraper, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.idle_consume, signals.spider_idle)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def idle_consume(self):
        reqs = self.start_requests()
        if not reqs:
            return
        for req in reqs:
            # self.crawler.engine.schedule(req, self)
            self.crawler.engine.crawl(req)
            # raise DontCloseSpider
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
            # upload_file(f'result_{current_time}.csv') if CLOUD_MODE else None
        else:
            filenames = os.listdir(self.data_storage_path)
            pattern = r'\w*_result_\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}.csv'
            for filename in filenames:
                match = re.search(pattern, filename, re.I)
                if match:
                    previous_result_path = f"{self.data_storage_path}{match.group()}"
                    os.remove(previous_result_path)
            append_df.to_csv(self.data_storage_path + f'{self.auth}_result_{current_time}.csv', index=False)
            # upload_file(f'{self.auth}_result_{current_time}.csv') if CLOUD_MODE else None
            # send_emails(self.auth)

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
        # sequential.
        self.index += 1
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
            #yield SeleniumRequest(url=url, callback=self.parse_data_item, meta={'app_df': app_df})
            yield SeleniumRequest(url=url, callback=self.parse_func, meta={'app_df': app_df})
        except IndexError:
            print("list is empty.")
            return

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
    details_dict = {'Application Number': 'uid',  # Cherwell & Crawley & Fylde
                    # 'Reference': 'uid',  # Non-Empty
                    # 'Application Reference': 'uid',  # New Duplicate [Derby]
                    #'Planning Portal Reference': 'other_fields.planning_portal_id',  # New [Derby]
                    #'Alternative Reference': 'altid',
                    'Proposal': 'description',  # A & Cherwell & Crawley & Fylde
                    'Application Type': 'other_fields.application_type',  # A & Cherwell & Crawley & Fylde
                    'Type': 'other_fields.application_type', # Redcar
                    'Status': 'other_fields.status',  # A & Cherwell & Crawley & Fylde

                    'Application Location': 'address',  # A
                    'Location':             'address',  # Cherwell & Crawley
                    'Location Address':     'address',  # Fylde
                    'Easting':  'other_fields.easting',  # A
                    'Northing': 'other_fields.northing',  # A

                    'Community Council':    'other_fields.parish',  # A
                    'Parish':               'other_fields.parish',  # Cherwell & Fylde
                    'Parish(es)':           'other_fields.parish',  # Essex
                    'Ward':                 'other_fields.ward_name',  # Cherwell & Fylde
                    'Electoral Division(s)':'other_fields.ward_name',  # Essex
                    'Electoral Division':   'other_fields.ward_name',  # Lancashire
                    'Electoral Divisions(s)': 'other_fields.ward_name',  # Lincolnshire
                    'District':             'other_fields.district',  # Cumbria
                    'District(s)':          'other_fields.district',  # Essex
                    'District Reference':   'other_fields.district',  # Leicestershire
                    #'Local Review Body Status':    'other_fields.local_review_body_status',  # New*
                    #'Local Review Body Decision':  'other_fields.local_review_body_decision'  # New*
                    'Building Control Application': 'other_fields.building_control_application',  # A . New
                    'Environmental Statement': 'other_fields.environmental_assessment',  # A
                    'Departure': 'other_fields.departure',  # A . New
                    # --- --- --- Decision --- --- ---
                    'Decision':             'other_fields.decision',  # A & Cherwell [Main Details] & Crawley [Main Details]
                    'Decision Date':        'other_fields.decision_issued_date',  # A & Fylde
                    'Committee / Delegated Decision Date': 'other_fields.decision_issued_date',  # Essex
                    'Decision Issued Date': 'other_fields.decision_issued_date',  # B
                    'Issue Date':           'other_fields.decision_issued_date',  # Crawley
                    'Decision Notice Issued Date': 'other_fields.decision_issued_date',  # Lincolnshire

                    'Decision Type':                                    'other_fields.expected_decision_level',  # A
                    'Decision Level':                                   'other_fields.expected_decision_level',  # B & Crawley
                    'Committee / Delegated Decision':                   'other_fields.expected_decision_level',  # Essex
                    'Expected Decision Level':                          'other_fields.expected_decision_level',  # Fylde
                    'Committee Decision / Delegated to Chief Officer':  'other_fields.expected_decision_level',  # Lancashire
                    'Committee/Delegated Decision':                     'other_fields.expected_decision_level',  # Lincolnshire
                    # --- --- --- Applicant/Agent/Officer Info --- --- ---
                    'Applicant Name':       'other_fields.applicant_name',  # NorthDevon
                    'Applicants Name':      'other_fields.applicant_name',  # A
                    'Applicant':            'other_fields.applicant_name',  # Cherwell & Crawley & Fylde
                    "Applicant's Address":  'other_fields.applicant_address',  # Cherwell & Crawley
                    'Applicant Address':    'other_fields.applicant_address',  # Redcar

                    'Agent Name':       'other_fields.agent_name',  # Leicester
                    'Agents Name':      'other_fields.agent_name',  # A
                    'Agent':            'other_fields.agent_name',  # Cherwell & Crawley & Fylde
                    'Agent Address':    'other_fields.agent_address',  # A & Fylde
                    "Agent's Address":  'other_fields.agent_address',  # Cherwell & Crawley
                    "Agents's Address": 'other_fields.agent_address',  # Cherwell & Crawley

                    'Case Officer':     'other_fields.case_officer',  # Cherwell & Crawley & Fylde
                    'Officer':          'other_fields.case_officer',  # A
                    'Planning Officer': 'other_fields.case_officer',  # Lancashire
                    # --- --- --- Dates --- --- ---
                    'Received':                 'other_fields.date_received',  # A
                    'Received Date':            'other_fields.date_received',  # Cherwell
                    'Application Received Date':'other_fields.date_received',  # Fylde
                    'Registered Date':          'other_fields.date_received',  # Crawley
                    'Valid Application Received':  'other_fields.date_received',  # Cumbria
                    'Date Received':            'other_fields.date_received', # Essex

                    'Validated':                'other_fields.date_validated',  # A
                    'Valid Date':               'other_fields.date_validated',  # Cherwell
                    'Date Valid':               'other_fields.date_validated',  # Essex
                    'Application Valid Date':   'other_fields.date_validated',  # Fylde
                    'Valid':                    'other_fields.date_validated', # Leicestershire

                    'Expires':      'other_fields.application_expires_date',  # A
                    'Expiry Date':  'other_fields.application_expires_date',  # NorthDevon

                    # Advertisement
                    'Advert Expiry': 'other_fields.latest_advertisement_expiry_date',  # Fylde

                    # Appeals
                    #'Appeal Status': 'other_fields.appeal_status',  # Cherwell
                    'Appeal Received Date': 'other_fields.appeal_date',  # Cherwell
                    'Appeal Date': 'other_fields.appeal_date',           # Lancashire
                    'Appeal Start Date':    'other_fields.appeal_start_date',  # Cherwell
                    'Appeal Decision':      'other_fields.appeal_result',  # Fylde
                    'Appeal':               'other_fields.appeal_result',  # Glamorgan
                    'Appeal Decision Date': 'other_fields.appeal_decision_date',  # Cherwell

                    # Comments
                    'Comments Due Date': 'other_fields.comment_expires_date',  # Cherwell & Crawley
                    'Comments Due Date**':'other_fields.comment_expires_date',  # WestNorthamptonshire
                    # Committee Date
                    'Committee Date':                   'other_fields.meeting_date',  # A & Cherwell [Main Details] & C [Main Details]
                    'Committee Date (if applicable)':   'other_fields.meeting_date',  # Fylde
                    'Committee Date (If applicable)':   'other_fields.meeting_date',  # Leicester
                    'Committee Date\n(if applicable)':  'other_fields.meeting_date', # WestmorlandFurness
                    #'Actual Committee Date': 'other_fields.meeting_date',
                    #'Actual Committee or Panel Date': 'other_fields.meeting_date',  # New Duplicate [Gedling]
                    #'Date of Committee Meeting': 'other_fields.meeting_date',  # New Duplicate [IOW]
                    #'Committee/Delegated List Date': 'other_fields.meeting_date',  #
                    'Committee Agenda': 'other_fields.committee_agenda' ,  # Fylde(New)

                    # Consultations:
                    'Consultation Start':           'other_fields.consultation_start_date',  # Essex
                    'Consultation Start Date':      'other_fields.consultation_start_date',  # Fylde
                    'Consultation Expiry':          'other_fields.consultation_end_date',  # Essex
                    'Consultation End':             'other_fields.consultation_end_date',  # Fylde
                    'Consultation Expiry Date':     'other_fields.consultation_end_date',  # Lincolnshire
                    'Public Consultation Start Date':'other_fields.public_consultation_start_date',  # Redcar
                    'Start of Public Consultation':  'other_fields.public_consultation_start_date',  # Leicestershire
                    'Public Consultation Expiry':   'other_fields.public_consultation_end_date',  # Cumbria
                    'Public Consultation End Date': 'other_fields.public_consultation_end_date',  # Redcar
                    'End of Public Consultation':   'other_fields.public_consultation_end_date',  # Leicestershire

                    # Determination

                    # Site Notice
                    'Site Notice Date':                 'other_fields.site_notice_start_date',  # Fylde
                    'Site Visited / Site Notice Date':  'other_fields.site_notice_start_date',  # MalvernHills
                    # Target Date
                    'Target Decision Date': 'other_fields.target_decision_date',  # Cherwell & Crawley

                    # Others
                    'Weekly List Date': 'other_fields.weekly_list_date',  # Cherwell
                    'Weekly List date': 'other_fields.weekly_list_date',  # Redcar
                    'Subject To Legal Agreements':              'other_fields.subject_to_legal_agreements',  # Essex
                    'Application subject to Legal Agreement':   'other_fields.subject_to_legal_agreements',  # Lancashire
                    'Completion of Legal Agreement':            'other_fields.completion_of_legal_agreements',  # Lancashire
                    'Local Member(s)':             'other_fields.local_member', # Essex
                    'PPRN': 'other_fields.pprn',  # Leicester
                    'Conservation Area': 'other_fields.conservation_area',  # WestmorlandFurness
                    'Listed Building Grade': 'other_fields.listed_building_grade',  # WestmorlandFurness
                    'Site Reference':   'other_fields.site_reference',  # Leicestershire
                    # --- --- --- Contact --- --- ---
                    'Phone':                            'Phone',  # Crawley
                    'Phone No':                         'Phone',  # Crawley
                    'Case Officer Telephone Number':    'Phone',  # Leicester
                    'Contact Telephone Number':         'Phone',  # Lancashire
                    'Case Officer Telephone':          'Phone',  # Leicestershire
                    'Email':                    'Email',  # Crawley
                    'Contact Email Address':    'Email'  # Lancashire
                    }

            # 61 ['other_fields.applicant_company', 'other_fields.id_type',
            # 'other_fields.district', 'other_fields.docs_url', 'other_fields.comment_date',
            # 'other_fields.n_dwellings', 'other_fields.appeal_status', 'app_type', 'other_fields.neighbour_consultation_start_date',
            # 'other_fields.decision_date', 'postcode', 'other_fields.n_documents',
            # 'reference', 'other_fields.neighbour_consultation_end_date', 'other_fields.n_constraints',

            # 'other_fields.n_comments', 'associated_id',
            # 'app_state', 'other_fields.appeal_reference', 'other_fields.uprn',
            # 'other_fields.latest_advertisement_expiry_date', 'start_date', 'consulted_date',
            # 'other_fields.planning_portal_id', 'other_fields.permission_expires_date', 'decided_date',
            #  'other_fields.decision_published_date',

            # 'other_fields.last_advertised_date',
            # 'other_fields.n_statutory_days', 'other_fields.development_type', 'other_fields.date_validated', 'other_fields.target_decision_date',
            # 'altid', 'other_fields.first_advertised_date', 'other_fields.map_url',,
            # 'app_size', 'other_fields.appeal_type', 'other_fields.site_notice_end_date', 'other_fields.decided_by']

    # For parse_data_item A & B & C
    def setup_storage_path(self, app_df):
        folder_name = str(app_df.at['name'])
        folder_name = replace_invalid_characters(folder_name)
        folder_path = f"{self.data_storage_path}{folder_name}/"
        print(folder_path) if PRINT else None
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
        upload_folder(f"{self.data_upload_path}{folder_name}/") if CLOUD_MODE else None
        return folder_name

    def set_default_items(self, app_df):
        app_df['other_fields.n_comments_public_total_consulted'] = 'Not available'
        app_df['other_fields.n_comments_public_received'] = 0  # ***
        app_df['other_fields.n_comments_public_objections'] = 'Not available'
        app_df['other_fields.n_comments_public_supporting'] = 'Not available'
        app_df['other_fields.n_comments_consultee_total_consulted'] = 'Not available'
        app_df['other_fields.n_comments_consultee_responded'] = 0  # ***
        app_df.at['other_fields.n_comments'] = 0  # ***
        app_df['other_fields.n_public_notices'] = 'Not available'  # New

        app_df.at['other_fields.n_constraints'] = 'Not available'
        app_df.at['other_fields.n_documents'] = 0  # ***
        app_df.at['other_fields.uprn'] = 'Not available'
        return app_df

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

    """
    data items
    """
    # For Bridgend x1, Glamorgan x2, Crawley x1, Cumbria x1, Essex x2, Kent x1, Lancashire x2, Lincolnshire x1, Somerset x1
    def scrape_data_items(self, app_df, items, item_values):
        for item, value in zip(items, item_values):
            item_name = item.text.strip()
            data_name = self.details_dict[item_name]
            item_value = value.text.strip()
            # print(i, item_name, item_value, type(item_name))
            # if data_name in self.app_dfs.columns:
            try:
                app_df.at[data_name] = item_value
                print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
            # New
            except KeyError:
                app_df[data_name] = item_value
                print(f"    <{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
        return app_df
    # app_df = self.scrape_data_items(app_df, items, item_values)

    # For Leicestershire x1, Crawley x1
    def scrape_data_items_including_contacts(self, app_df, items, item_values, folder_name, contact_items=['Phone', 'Email']):
        contact_dict = {}
        for item, value in zip(items, item_values):
            item_name = item.text.strip()
            data_name = self.details_dict[item_name]
            item_value = value.text.strip()
            # print(i, item_name, item_value, type(item_name))
            # if data_name in self.app_dfs.columns:
            try:
                if data_name in contact_items and len(item_value) > 0:
                    contact_dict[data_name] = [item_value]
                    print(f"    <{item_name}> scraped (contact): {item_value}") if PRINT else None
                else:
                    app_df.at[data_name] = item_value
                    print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
            # New
            except KeyError:
                app_df[data_name] = item_value
                print(f"    <{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
        if len(contact_dict) > 0:
            contact_df = pd.DataFrame(contact_dict)
            contact_df.to_csv(f"{self.data_storage_path}{folder_name}/contact.csv", index=False)
        return app_df
    # app_df = self.scrape_data_items_including_contacts(app_df, items, item_values, folder_name)

    # For Bridgend x1, Lancashire x1
    def scrape_data_items_including_contacts_and_checkbox(self, app_df, items, item_values,
                                                          folder_name, contact_items=['Phone', 'Email'], checkbox_items=[]):
        contact_dict = {}
        for item, value in zip(items, item_values):
            item_name = item.text.strip()
            data_name = self.details_dict[item_name]
            item_value = value.text.strip()
            # print(i, item_name, item_value, type(item_name))
            # if data_name in self.app_dfs.columns:
            try:
                if data_name in contact_items and len(item_value) > 0:
                    contact_dict[data_name] = [item_value]
                    print(f"    <{item_name}> scraped (contact): {item_value}") if PRINT else None
                elif item_name in checkbox_items:
                    app_df[data_name] = value.find_element(By.XPATH, './input').is_selected()
                    print(f"    <{item_name}> scraped (checkbox): {app_df.at[data_name]}") if PRINT else None
                else:
                    app_df.at[data_name] = item_value
                    print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
            # New
            except KeyError:
                app_df[data_name] = item_value
                print(f"    <{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
        if len(contact_dict) > 0:
            contact_df = pd.DataFrame(contact_dict)
            contact_df.to_csv(f"{self.data_storage_path}{folder_name}/contact.csv", index=False)
        return app_df
    # app_df = self.scrape_data_items_including_contacts_and_checkbox(app_df, items, item_values, folder_name, checkbox_items=['Departure'])

    """
    consultations, constraints.
    """
    def unique_columns(self, column_names):
        count_dict = {}
        unique_names = []
        for item in column_names:
            if item in count_dict:
                unique_names.append(f"{item}.{count_dict[item]}")
                count_dict[item] += 1
            else:
                unique_names.append(item)
                count_dict[item] = 1
        return unique_names

    ### Case 1 ### Try the only table in a tab, catch NoSuchElement.
    # Multi-columns #
    # td: For Glamorgan x5, Essex x1, Lincolnshire x1.
    # trtd: For Bridgend x3
    # div : For Redcar x3
    def scrape_for_csv(self, csv_name, table_columns, table_items, folder_name, path='td'):
        content_dict = {}
        column_names = [column.text.strip() for column in table_columns]
        column_names = self.unique_columns(column_names)
        # print(f'{table_name}, {len(table_items)} items with column names: ', column_names) if PRINT else None
        n_columns = len(column_names)

        for column_index in range(n_columns):
            #content_dict[column_names[column_index]] = [table_item.find_element(By.XPATH, f'./td[{column_index+1}]').text.strip() for table_item in table_items]
            content_dict[column_names[column_index]] = [table_item.find_element(By.XPATH, f'./{path}[{column_index+1}]').text.strip() for table_item in table_items]

        content_df = pd.DataFrame(content_dict)
        content_df.to_csv(f"{self.data_storage_path}{folder_name}/{csv_name}.csv", index=False)

    # Single column #
    # div[2]: For Redcar x1
    # td: For Cherwell/WestNorthamptonshire x1
    def scrape_for_csv_single(self, csv_name, column_name, table_items, folder_name, path='td'):
        content_dict = {column_name: [table_item.find_element(By.XPATH, f'./{path}').text.strip() for table_item in table_items]}
        content_df = pd.DataFrame(content_dict)
        content_df.to_csv(f"{self.data_storage_path}{folder_name}/{csv_name}.csv", index=False)

    ### Case 2 ### Multiple tables in a tab, including empty tables.
    # table (tbody/tr), column (th), item (td): For Cherwell/WestNorthamptonshire x1
    def scrape_multi_tables_for_csv(self, csv_names, tables, folder_name, table_path='tbody/tr', column_path='th', item_path='td'):
        n_table_items = []
        for table_index, table in enumerate(tables):
            # table_name = table_names[table_index].text.strip().lower()
            table_rows = table.find_elements(By.XPATH, f'./{table_path}')
            table_columns = table_rows[0].find_elements(By.XPATH, f'./{column_path}')
            if len(table_columns) > 0:
                table_items = table_rows[1:]
                self.scrape_for_csv(csv_names[table_index], table_columns, table_items, folder_name, path=item_path)
                print(f'{csv_names[table_index]}, {len(table_items)} items') if PRINT else None
                n_table_items.append(len(table_items))
            else:
                table_item = table_rows[0].find_element(By.XPATH, f'./{item_path}').text.strip()
                print(f"{csv_names[table_index]} <NULL>: {table_item}") if PRINT else None
                n_table_items.append(0)
        return n_table_items

    # table (tbody/tr), name (td/h2), column (th), item (td): For Cherwell/WestNorthamptonshire x1
    def scrape_multi_tables_for_csv_inner_tablename_with_prefix(self, csv_prefix, tables, folder_name, table_path='tbody/tr', name_path='td/h2', column_path='th', item_path='td'):
        #n_table_items = []
        for table_index, table in enumerate(tables):
            table_rows = table.find_elements(By.XPATH, f'./{table_path}')
            table_name = table_rows[0].find_element(By.XPATH, f'./{name_path}').text.strip().lower()
            table_columns = table_rows[1].find_elements(By.XPATH, f'./{column_path}')
            if len(table_columns) > 0:
                table_items = table_rows[2:]
                self.scrape_for_csv(csv_prefix+table_name, table_columns, table_items, folder_name, path=item_path)
                print(f'{csv_prefix+table_name}, {len(table_items)} items') if PRINT else None
                #n_table_items.append(len(table_items))
            else:
                table_item = table_rows[0].find_element(By.XPATH, f'./{item_path}').text.strip()
                print(f"{csv_prefix+table_name} <NULL>: {table_item}") if PRINT else None
                #n_table_items.append(0)
        #return n_table_items

    # table (div), name (label), column (div), item (div), pre_item (div): For Crawley (No empty table) x1
    def scrape_multi_tables_for_csv_inner_tablename(self, table_name_dict, tables, folder_name, table_path='div', name_path='label', column_path='div', item_path='div', pre_item_path='div'):
        n_table_items = []
        csv_names = []
        for table_index, table in enumerate(tables):
            table_rows = table.find_elements(By.XPATH, f'./{table_path}')
            table_name = table_rows[0].find_element(By.XPATH, f'./{name_path}').text.strip().lower()
            table_columns = table_rows[1].find_elements(By.XPATH, f'./{column_path}')

            csv_name = table_name_dict[table_name]
            csv_names.append(csv_name)
            if len(table_columns) > 0:
                table_items = table_rows[2:] if pre_item_path == '' else table_rows[2].find_elements(By.XPATH, f'./{pre_item_path}')
                self.scrape_for_csv(csv_name, table_columns, table_items, folder_name, path=item_path)
                print(f'{csv_name}, {len(table_items)} items') if PRINT else None
                n_table_items.append(len(table_items))
            else:
                table_item = table_rows[0].find_element(By.XPATH, f'./{item_path}').text.strip()
                print(f"{csv_name} <NULL>: {table_item}") if PRINT else None
                n_table_items.append(0)
        return n_table_items, csv_names


    """
    documents
    """
    def create_item(self, driver, folder_name, file_urls, document_names):
        if not os.path.exists(self.failed_downloads_path + folder_name):
            os.mkdir(self.failed_downloads_path + folder_name)

        item = DownloadFilesItem()
        item['file_urls'] = file_urls
        item['document_names'] = document_names

        cookies = driver.get_cookies()
        print("cookies:", cookies) if PRINT else None
        item['session_cookies'] = cookies
        return item

    ### Case 1 ### Multiple Tables
    # [date, description]: For Glamorgan, Essex, Lancashire, Somerset, Lincolnshire(file)
    # [date, type, description]: For Cherwell/WestNorthamptonshire, Leicester
    def get_column_indexes(self, columns, keywords):
        n_columns = len(columns)
        n_keywords = len(keywords)
        column_indexes = [n_columns + 1,] * n_keywords
        for keyword_index, keyword in enumerate(keywords):
            for column_index, column in enumerate(columns):
                if keyword in str.lower(column.text.strip()):
                    column_indexes[keyword_index] = column_index + 1
                    break
        if PRINT:
            print_str = '    Columns: '
            for keyword_index, keyword in enumerate(keywords):
                print_str += f'{keyword} column {column_indexes[keyword_index]}/{n_columns}. '
            print(print_str)
        return column_indexes
    # [date_column, description_column] = self.get_column_indexes(columns, keywords=['date', 'file name'])

    # For Glamorgan, Essex, Lancashire, Somerset, Lincolnshire(file)
    # (Multi tabs without column names) For Cumbria, Leicestershire
    def rename_document_date_desc(self, document_item, document_name, document_type='-', description_column=2, date_column=3, path='td'):
        #item_extension = file_url.split('.')[-1]
        #document_name = f"uid={n_documents}.{item_extension}"
        try:
            document_description = document_item.find_element(By.XPATH, f'./{path}[{description_column}]').text.strip()
            document_name = f"desc={document_description}&{document_name}"
        except NoSuchElementException:
            pass
        #document_type = document_table_name
        document_name = f"type={document_type}&{document_name}"
        try:
            document_date = document_item.find_element(By.XPATH, f'./{path}[{date_column}]').text.strip()
            document_name = f"date={document_date}&{document_name}"
        except NoSuchElementException:
            pass
        return document_name
    # document_name = self.rename_document_date_desc(document_item, document_name, document_type=document_table_name, description_column=description_column, date_column=date_column, path='td'))

    # For Cherwell/WestNorthamptonshire
    def rename_document(self, document_item, document_name, description_column=2, type_column=1, date_column=3, path='td'):
        try:
            document_description = document_item.find_element(By.XPATH, f'./{path}[{description_column}]').text.strip()
            document_name = f"desc={document_description}&{document_name}"
        except NoSuchElementException:
            pass
        document_type = document_item.find_element(By.XPATH, f'./{path}[{type_column}]').text.strip()
        document_name = f"type={document_type}&{document_name}"
        try:
            document_date = document_item.find_element(By.XPATH, f'./{path}[{date_column}]').text.strip()
            document_name = f"date={document_date}&{document_name}"
        except NoSuchElementException:
            pass
        return document_name
    # document_name = self.rename_document(document_item, document_name, description_column, type_column, date_column, path='td')

    ### Case 2 ### Single Table with headers and document items mixed.
    # For Crawley (table_path = '//*[@id="documents"]/div/div[2]/table/tbody'),
    #      Redcar (table_path = '//*[@id="documents"]/div[2]/table/tbody')
    #        Kent (table_path = '//*[@id="main"]/div[1]/div/div/div[2]/div[1]/table/tbody')
    # default paths: row: './tr', header: './th', url & description: './td[1]/label/span/a', date: './td[3]'.
    #        Kent    row: './tr', header: './th', url & description: './td[2]/a', date: './td[3]'.
    # Not encapsulated: Leicester
    def get_documents_from_single_table(self, driver, app_df, folder_name, tab_index,
                                        table_path='//*[@id="documents"]/div[2]/table/tbody', description_path='./td[1]/label/span/a'):
        try:
            document_table = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'{table_path}')))
            #document_table = driver.find_element(By.XPATH, '//*[@id="documents"]/div[2]/table/tbody')
        #except NoSuchElementException:  # No documents found for this application
        except TimeoutException:  # No documents found for this application
            app_df.at['other_fields.n_documents'] = 0
            print(f"\n{tab_index+2}. <NULL> Document Tab: {app_df.at['other_fields.n_documents']} items.") if PRINT else None
            return 0, [], []

        row_items = document_table.find_elements(By.XPATH, './tr')  # include headers.
        headers = document_table.find_elements(By.CLASS_NAME, 'header')
        print(f"\n{tab_index+2}. Document Tab: {len(headers)} tables, including {len(row_items)-len(headers)} documents.") if PRINT else None

        n_documents, file_urls, document_names = 0, [], []
        for row_item in row_items:
            try:
                header_name = row_item.find_element(By.XPATH, './th').text.strip()
                print(f"header: {header_name}") if PRINT else None
            except NoSuchElementException:
                n_documents += 1
                file_url = row_item.find_element(By.XPATH, description_path).get_attribute('href')
                file_urls.append(file_url)

                item_identity = f"{n_documents}.{file_url.split('.')[-1]}"
                document_description = row_item.find_element(By.XPATH, description_path).text.strip()
                document_date = row_item.find_element(By.XPATH, './td[3]').text.strip()

                document_name = f"date={document_date}&type={header_name}&desc={document_description}&uid={item_identity}"
                print(f"    Document {n_documents}: {document_name}") if PRINT else None
                document_name = replace_invalid_characters(document_name)
                document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
        app_df.at['other_fields.n_documents'] = n_documents
        print(f'Total documents: {n_documents}') if PRINT else None
        return n_documents, file_urls, document_names, app_df
    # [n_documents, file_urls, document_names, app_df] = self.get_documents_from_single_table(driver, app_df, folder_name, tab_index, table_path='//*[@id="documents"]/div[2]/table/tbody', description_path='./td[1]/label/span/a')

    """ Bridgend 
    Features: [Details], [Other Details], [Decision], [Consultees], [Documents], [Public Notices]
        1.Encapsulated(2/3): Tab [Details] uses empty-try (old implementations).
        Tab Main Details: Framework {item: dt, value: dd}
        
        2.Encapsulated(3/3): [Consultations, Neighbours, Public Notices]
        
        3.Doc system: Multi-tables, not adaptive (old implementations).
    """
    def parse_data_item_A(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        ### Ensure the page content is loaded.
        try:
            if 'Disclaimer' in response.xpath('//*[@id="main-content"]/div/div/section/div/div/div/h1/text()').get():
                print('Click: Agree the disclaimer.')
                driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div/section/div/div/div/form/div/input').click()
        except TypeError:
            pass
        print(f"parse_data_item_Bridgend, scraper name: {scraper_name}")

        #self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)
        try:
            # --- --- --- Details --- --- ---
            base_display_data_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="details"]/dl')))
            items = base_display_data_list.find_elements(By.XPATH, './dt')
            item_values = base_display_data_list.find_elements(By.XPATH, './dd')
            n_items = len(items)
            print(f"\n1. Details Tab: {n_items}")  # if PRINT else None #print(f"Details Tab: {n_items}")

            for i, item in enumerate(items):
                item_name = item.text.strip()
                data_name = self.details_dict[item_name]
                item_value = item_values[i].text.strip()
                #print(i, item_name, item_value, type(item_name))
                # if data_name in self.app_dfs.columns:
                try:
                    # Empty
                    if self.is_empty(app_df.at[data_name]) or app_df.at[data_name] == 'See source':
                        app_df.at[data_name] = item_value
                        print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                    # Filled
                    else:
                        print(f"    <{item_name}> filled: {app_df.at[data_name]}") if PRINT else None
                # New
                except KeyError:
                    app_df[data_name] = item_value
                    print(f"    <{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
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
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return
                # print('--- --- test --- ---')

        tabs = driver.find_elements(By.XPATH, '//*[@id="myTopnav"]/a')[:-1]
        app_df = self.set_default_items(app_df)

        for tab_index, tab in enumerate(tabs[1:]):
            tab.click()
            tab_name = tab.text.strip()
            # --- --- --- Other Details --- --- ---
            if 'Other Details' in tab_name:
                base_display_data_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="otherdetails"]/dl')))
                #base_display_data_list = driver.find_element(By.XPATH, '//*[@id="otherdetails"]/dl')
                items = base_display_data_list.find_elements(By.XPATH, './dt')
                item_values = base_display_data_list.find_elements(By.XPATH, './dd')
                n_items = len(items)
                print(f"\n{tab_index+2}. Other Details Tab: {n_items}") if PRINT else None
                app_df = self.scrape_data_items_including_contacts_and_checkbox(app_df, items, item_values, folder_name,
                                                                                checkbox_items=['Environmental Statement', 'Departure'])
            # --- --- --- Decision --- --- ---
            elif 'Decision' in tab_name:
                base_display_data_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="decision"]/dl')))
                #base_display_data_list = driver.find_element(By.XPATH, '//*[@id="decision"]/dl')
                items = base_display_data_list.find_elements(By.XPATH, './dt')
                item_values = base_display_data_list.find_elements(By.XPATH, './dd')
                n_items = len(items)
                print(f"\n{tab_index+2}. Decision Tab: {n_items}") if PRINT else None
                app_df = self.scrape_data_items(app_df, items, item_values)
            # --- --- --- Consultees --- --- ---
            elif 'Consultee' in tab_name:  # Adaptive
                def parse_consultees():
                    try:
                        consultees_table = driver.find_element(By.XPATH, '//*[@id="consultees"]/div/table')
                        table_items = consultees_table.find_elements(By.XPATH, './tbody')
                        n_items = len(table_items)
                        print(f"\n{tab_index+2}. Consultees Tab: {n_items}") if PRINT else None
                        if n_items > 0:
                            app_df.at['other_fields.n_comments_consultee_responded'] = n_items
                            app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at[
                                'other_fields.n_comments_consultee_responded']
                            table_columns = consultees_table.find_elements(By.XPATH, './thead/tr/th')
                            #scrape_for_csv(csv_name='consultee comments', table_columns=table_columns, table_items=table_items)
                            self.scrape_for_csv(csv_name='consultee comments', table_columns=table_columns, table_items=table_items,
                                                folder_name=folder_name, path='tr/td')
                    except NoSuchElementException:
                        print(f"\n{tab_index+2}. " + driver.find_element(By.XPATH, '//*[@id="consultees"]/p').text.strip()) # No Consultations found for this Application
                parse_consultees()
            # --- --- --- Neighbours --- --- ---
            elif 'Neighbour' in tab_name:  # Adaptive * need tests
                def parse_neighbour():
                    try:
                        neighbours_table = driver.find_element(By.XPATH, '//*[@id="neighbours"]/div/table')
                        table_items = neighbours_table.find_elements(By.XPATH, './tbody')
                        n_items = len(table_items)
                        print(f"\n{tab_index+2}. Neighbours Tab: {n_items}") if PRINT else None
                        if n_items > 0:
                            app_df.at['other_fields.n_comments_public_received'] = n_items
                            app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at[
                                'other_fields.n_comments_consultee_responded']
                            table_columns = neighbours_table.find_elements(By.XPATH, './thead/tr/th')
                            self.scrape_for_csv(csv_name='neighbour comments', table_columns=table_columns, table_items=table_items,
                                                folder_name=folder_name, path='tr/td')
                    except NoSuchElementException:
                        print(f"\n{tab_index+2}." + driver.find_element(By.XPATH, '//*[@id="neighbours"]/p').text.strip())  # No Consultations found for this Application
                parse_neighbour()
            # --- --- --- Documents --- --- ---
            elif 'Document' in tab_name:
                def get_documents():
                    try:
                        document_table_list = driver.find_element(By.XPATH, '//*[@id="documents"]/div/table')
                        document_tables = document_table_list.find_elements(By.XPATH, './tbody')
                        n_tables = len(document_tables)
                        print(f"\n{tab_index+2}. Document Tab: {n_tables} tables") if PRINT else None

                        n_documents, file_urls, document_names = 0, [], []
                        for table_index, document_table in enumerate(document_tables):  # document_table:  //*[@id="documents"]/div/table/tbody[table_index+1]
                            document_table_name = document_table.find_element(By.XPATH, './tr[1]/th').text.strip() # ./tr[1]/th/text()
                            document_items = document_table.find_elements(By.XPATH, './tr')[1:]
                            n_table_documents = len(document_items)
                            print(f"Table {table_index+1}: {document_table_name}, including {n_table_documents} documents.") if PRINT else None
                            for document_index, document_item in enumerate(document_items):
                                file_url = document_item.find_element(By.XPATH, './td[3]/a').get_attribute('href')
                                file_urls.append(file_url)

                                document_type = document_item.find_element(By.XPATH, './td[2]').text.strip() # document_item./td[2]
                                document_description = document_item.find_element(By.XPATH, './td[3]/a').text.strip()
                                document_date = document_item.find_element(By.XPATH, './td[4]').text.strip() # document_item./td[4]
                                item_identity = file_url.split('=')[-1]  # includes extension such as .pdf
                                document_name = f"date={document_date}&type={document_type}&desc={document_description}&uid={item_identity}"
                                print(f"    Document {document_index+1}: {document_name}") if PRINT else None
                                document_name = replace_invalid_characters(document_name)
                                document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                            n_documents += n_table_documents
                        app_df.at['other_fields.n_documents'] = n_documents
                        print(f'Total documents: {n_documents}') if PRINT else None
                        return n_documents, file_urls, document_names
                    except NoSuchElementException:
                        print(f"\n{tab_index+2}. " + driver.find_element(By.XPATH, '//*[@id="documents"]/div/div[2]/label').text.strip())  # No documents found for this application
                        return 0, [], []
                n_documents, file_urls, document_names = get_documents()
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            # --- --- --- Public Notice --- --- ---
            elif 'Public Notice' in tab_name:  # Adaptive
                def parse_public_notice():
                    try:
                        public_notices_table = driver.find_element(By.XPATH, '//*[@id="publicnotices"]/div/table')
                        table_items = public_notices_table.find_elements(By.XPATH, './tbody')
                        n_items = len(table_items)
                        print(f"\n{tab_index+2}. Public Notices Tab: {n_items}") if PRINT else None
                        if n_items > 0:
                            app_df.at['other_fields.n_public_notices'] = n_items
                            table_columns = public_notices_table.find_elements(By.XPATH, './thead/tr/th')
                            self.scrape_for_csv(csv_name='public notice', table_columns=table_columns, table_items=table_items,
                                                folder_name=folder_name, path='tr/td')
                    except NoSuchElementException:
                        print(f"\n{tab_index+2}. " + driver.find_element(By.XPATH, '//*[@id="publicnotices"]/p').text.strip())  # No Public Notices found for this Application
                parse_public_notice()
            else:
                print(f'Unknown tab: {tab_name}')
                assert 0 == 1
        self.ending(app_df)



    """ Glamorgan's overall framework is similar to A (Bridgend), but the implementation of Glamorgan framework details is much more complex.
    https://vogonline.planning-register.co.uk/Planning/Display/2006/00707/FUL
    Features: [Details], [Other Details], [Decision], [Consultees], [Documents], [Public Notices], [Constraints]
        Tabs have no id label.
        1.Encapsulated(2/3): Tab [other details] has a bug.
        Tab Main Details: Framework {item: div/dt or dt, value: div/dd or dd}
        
        2.Encapsulated(5/5): [Conditions, Consultations, Neighbours, Public Notices, Constraints]
        
        3.Encapsulated Doc system: Multi-tables with types as sub table names. [Similar to Essex except column names] 
            #Shared Columns
            #Type1
            #    Document items [date, description(with links)].
            #Type2
            #    Document items [date, description(with links)].
    """
    def parse_data_item_Glamorgan(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        ### Ensure the page content is loaded.
        try:
            if 'Disclaimer' in response.xpath('//*[@id="ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField"]/div/h4/text()').get():
                print('Click: Agree the disclaimer.')
                driver.find_element(By.XPATH, '//*[@id="ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField"]/div/form/div/input').click()
        except TypeError:
            pass
        print(f"parse_data_item_Glamorgan, scraper name: {scraper_name}")

        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)

        def collect_items(tab_path):
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, tab_path)))
            items = tab_panel.find_elements(By.XPATH, './div/dt')
            items.extend(tab_panel.find_elements(By.XPATH, './dt'))
            item_values = tab_panel.find_elements(By.XPATH, './div/dd')
            item_values.extend(tab_panel.find_elements(By.XPATH, './dd'))
            return items, item_values

        try:
            # --- --- --- Details --- --- ---
            items, item_values = collect_items('//*[@id="content"]/div[3]/div[1]/dl')
            n_items = len(items)
            print(f"\n1. Details Tab: {n_items}.")  # if PRINT else None
            app_df = self.scrape_data_items(app_df, items, item_values)

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
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return
                # print('--- --- test --- ---'
        tabs = driver.find_elements(By.XPATH, '//*[@id="myTopnav"]/a')[:-1]
        app_df = self.set_default_items(app_df)

        for tab_index, tab in enumerate(tabs[1:]):
            tab.click()
            tab_name = tab.text.strip()
            # --- --- --- Other Details --- --- ---
            if 'Other Details' in tab_name:
                def parse_other_details():
                    items, item_values = collect_items(f'//*[@id="content"]/div[3]/div[{tab_index+2}]/dl')
                    items2, item_values2 = collect_items(f'//*[@id="content"]/div[3]/div[{tab_index+2}]/dl[2]')
                    items.extend(items2)
                    item_values.extend(item_values2)
                    n_items = len(items)
                    print(f"\n{tab_index+2}. Other Details Tab: {n_items}.") if PRINT else None

                    contact_dict = {}
                    for item, value in zip(items, item_values):
                        item_name = item.text.strip()
                        if item_name == 'Other Details':
                            item_name = 'Officer'  # Fix the bug in the LA portal
                        data_name = self.details_dict[item_name]
                        item_value = value.text.strip()
                        # print(i, item_name, data_name, item_value, type(item_name)) if PRINT else None
                        # if data_name in self.app_dfs.columns:
                        try:
                            if data_name in ['Phone', 'Email']:
                                if len(item_value) > 0:
                                    contact_dict[data_name] = [item_value]
                                    print(f"    <{item_name}> scraped: {item_value}") if PRINT else None
                            elif item_name in ['Environmental Statement', 'Departure']:
                                app_df[data_name] = value.find_element(By.XPATH, './input').is_selected()
                                print(f"    <{item_name}> scraped (checkbox): {app_df.at[data_name]}") if PRINT else None
                            else:
                                app_df.at[data_name] = item_value
                                print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                        # New
                        except KeyError:
                            app_df[data_name] = item_value
                            print(f"<{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
                    if len(contact_dict) > 0:
                        contact_df = pd.DataFrame(contact_dict)
                        contact_df.to_csv(f"{self.data_storage_path}{folder_name}/contact.csv", index=False)
                parse_other_details()
            # --- --- --- Decision --- --- ---
            elif 'Decision' in tab_name:
                items, item_values = collect_items(f'//*[@id="content"]/div[3]/div[{tab_index+2}]/dl')
                n_items = len(items)
                print(f"\n{tab_index+2}. Decision Tab: {n_items}.") if PRINT else None
                app_df = self.scrape_data_items(app_df, items, item_values)

                # get conditions:
                has_condition = True
                try:
                    condition_table = driver.find_element(By.XPATH, f'//*[@id="content"]/div[3]/div[{tab_index+2}]/table')
                except NoSuchElementException:
                    print(f"No condtion items.") if PRINT else None
                    has_condition = False

                if has_condition:
                    condition_items = condition_table.find_elements(By.XPATH, './tbody/tr')
                    if 'NO CONDITIONS' in condition_items[0].find_element(By.XPATH, './td[2]').text.strip(): # no conditions
                        assert len(condition_items) == 1
                        print(f"No condtion items.")
                    else:
                        condition_columns = condition_table.find_elements(By.XPATH, './thead/tr/th')
                        self.scrape_for_csv(csv_name='condition_details', table_columns=condition_columns, table_items=condition_items,
                                            folder_name=folder_name, path='td')
            # --- --- --- Consultees --- --- ---
            elif 'Consultee' in tab_name:  # Adaptive
                def parse_consultees():
                    try:
                        consultees_table = driver.find_element(By.XPATH, f'//*[@id="content"]/div[3]/div[{tab_index+2}]/table')
                        table_items = consultees_table.find_elements(By.XPATH, './tbody/tr')
                        n_items = len(table_items)
                        print(f"\n{tab_index+2}. Consultees Tab: {n_items}") if PRINT else None
                        if n_items > 0:
                            app_df.at['other_fields.n_comments_consultee_responded'] = n_items
                            app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at[
                                'other_fields.n_comments_consultee_responded']
                            table_columns = consultees_table.find_elements(By.XPATH, './thead/tr/th')
                            self.scrape_for_csv(csv_name='consultee comments', table_columns=table_columns, table_items=table_items,
                                                folder_name=folder_name, path='td')
                    except NoSuchElementException: # No Consultations found for this Application
                        print(f"\n{tab_index+2}. " + driver.find_element(By.XPATH, f'//*[@id="content"]/div[3]/div[{tab_index+2}]/p').text.strip())
                parse_consultees()
            # --- --- --- Neighbours --- --- ---
            elif 'Neighbour' in tab_name:  # Adaptive * need tests
                def parse_neighbour():
                    try:
                        neighbours_table = driver.find_element(By.XPATH, f'//*[@id="content"]/div[3]/div[{tab_index+2}]/table')
                        table_items = neighbours_table.find_elements(By.XPATH, './tbody/tr')
                        n_items = len(table_items)
                        print(f"\n{tab_index+2}. Neighbours Tab: {n_items}") if PRINT else None
                        if n_items > 0:
                            app_df.at['other_fields.n_comments_public_received'] = n_items
                            app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at[
                                'other_fields.n_comments_consultee_responded']
                            table_columns = neighbours_table.find_elements(By.XPATH, './thead/tr/th')
                            self.scrape_for_csv(csv_name='neighbour comments', table_columns=table_columns, table_items=table_items,
                                                folder_name=folder_name, path='td')
                    except NoSuchElementException:   # No Neighbours responses found for this Application
                        print(f"\n{tab_index+2}. " + driver.find_element(By.XPATH, f'//*[@id="content"]/div[3]/div[{tab_index+2}]/p').text.strip())
                parse_neighbour()
            # --- --- --- Documents --- --- ---
            elif 'Document' in tab_name:
                def get_documents():
                    try:
                        document_table_list = driver.find_element(By.XPATH, f'//*[@id="content"]/div[3]/div[{tab_index+2}]/div/div/table')
                    except NoSuchElementException:  # No Attachments found for this Application
                        app_df.at['other_fields.n_documents'] = 0
                        print(f"\n{tab_index+2}. <NULL> Document Tab: {app_df.at['other_fields.n_documents']} items.") if PRINT else None
                        return 0, [], []

                    document_tables = document_table_list.find_elements(By.XPATH, './tbody')
                    n_tables = len(document_tables)
                    print(f"\n{tab_index+2}. Document Tab: {n_tables} tables") if PRINT else None

                    columns = document_table_list.find_elements(By.XPATH, './thead/tr/th')
                    [date_column, description_column] = self.get_column_indexes(columns, keywords=['date', 'file name'])

                    n_documents, file_urls, document_names = 0, [], []
                    for table_index, document_table in enumerate(document_tables):
                        document_table_name = document_table.find_element(By.XPATH, './tr[1]/th').text.strip()
                        document_items = document_table.find_elements(By.XPATH, './tr')[1:]
                        n_table_documents = len(document_items)
                        print(f"Table {table_index+1}: {document_table_name}, including {n_table_documents} documents.") if PRINT else None
                        for document_item in document_items:
                            n_documents += 1
                            file_url = document_item.find_element(By.XPATH, f'./td[{description_column}]/a').get_attribute('href')
                            file_urls.append(file_url)

                            item_extension = file_url.split('.')[-1]
                            document_name = f"uid={n_documents}.{item_extension}"
                            document_name = self.rename_document_date_desc(document_item, document_name, document_type=document_table_name,
                                                                           description_column=description_column, date_column=date_column, path='td')
                            # document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_extension}"
                            print(f"    Document {n_documents}: {document_name}") if PRINT else None
                            document_name = replace_invalid_characters(document_name)
                            document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                    app_df.at['other_fields.n_documents'] = n_documents
                    print(f'Total documents: {n_documents}') if PRINT else None
                    return n_documents, file_urls, document_names
                n_documents, file_urls, document_names = get_documents()
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            # --- --- --- Public Notice --- --- ---
            elif 'Public Notice' in tab_name:  # Adaptive
                def parse_public_notice():
                    try:
                        public_notices_table = driver.find_element(By.XPATH, f'//*[@id="content"]/div[3]/div[{tab_index+2}]/div/div/table')
                        table_items = public_notices_table.find_elements(By.XPATH, './tbody/tr')
                        n_items = len(table_items)
                        print(f"\n{tab_index+2}. Public Notices Tab: {n_items}") if PRINT else None
                        if n_items > 0:
                            app_df.at['other_fields.n_public_notices'] = n_items
                            table_columns = public_notices_table.find_elements(By.XPATH, './thead/tr/th')
                            self.scrape_for_csv(csv_name='public notice', table_columns=table_columns, table_items=table_items,
                                                folder_name = folder_name, path = 'td')
                    except NoSuchElementException: # No Public Notices found for this Application
                        print(f"\n{tab_index+2}. " + driver.find_element(By.XPATH, f'//*[@id="content"]/div[3]/div[{tab_index+2}]/p').text.strip())
                parse_public_notice()
            # --- --- --- Constraints --- --- ---
            elif 'Constraint' in tab_name:  # Adaptive
                def parse_constraint():
                    try:
                        constraint_table = driver.find_element(By.XPATH, f'//*[@id="content"]/div[3]/div[{tab_index+2}]/div/div/table')
                        table_items = constraint_table.find_elements(By.XPATH, './tbody/tr')
                        n_items = len(table_items)
                        print(f"\n{tab_index+2}. Constraints Tab: {n_items}") if PRINT else None
                        if n_items > 0:
                            app_df.at['other_fields.n_constraints'] = n_items
                            table_columns = constraint_table.find_elements(By.XPATH, './thead/tr/th')
                            self.scrape_for_csv(csv_name='constraints', table_columns=table_columns, table_items=table_items,
                                                folder_name=folder_name, path='td')
                    except NoSuchElementException:  # No Constraints found for this Application
                        print(f"\n{tab_index+2}. " + driver.find_element(By.XPATH, f'//*[@id="content"]/div[3]/div[{tab_index+2}]/p').text.strip())
                parse_constraint()
            else:
                print(f'Unknown tab: {tab_name}')
                assert 0 == 1
        self.ending(app_df)



    """ Redcar's overall framework is similar to A (Bridgend) and Glamorgan, but the implementation of Redcar framework details is much more complex.
     Features: [Details], [Other Details], [Decision], [Consultees], [Documents], [Public Notices]
        1. (0/3): OR, double contacts.
        Tab Main Details: Framework {item: div/label, value: div/div/input OR div/div/textarea}
        Tab Main Details and tab Other Details have Agent contact and Officer contact {Phone, Email}, respectively.
        Tab Decision has conditions.
        
        2. Encapsulated(4/4): [Conditions(single), Consultations, Neighbours, Public Notices]
        
        3. Encapsulated Doc system <get_documents_from_single_table>: 
        Single table with headers and document items mixed. Framework [date, description(with links)].
     """
    def parse_data_item_Redcar(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        ### Ensure the page content is loaded.
        try:
            if 'Disclaimer' in response.xpath('//*[@id="ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField"]/div/h4/text()').get():
                print('Click: Agree the disclaimer.')
                driver.find_element(By.XPATH, '//*[@id="ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField"]/div/form/div/input').click()
        except TypeError:
            pass
        print(f"parse_data_item_Redcar, scraper name: {scraper_name}")

        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)

        def collect_items(tab_path, skip_condition=False):
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, tab_path)))
            items = []
            if skip_condition:
                data_rows = tab_panel.find_elements(By.XPATH, './div')[:-1]
            else:
                data_rows = tab_panel.find_elements(By.XPATH, './div')
            for data_row in data_rows:
                data_columns = data_row.find_elements(By.XPATH, './div')
                for data_column in data_columns:
                    data_items = data_column.find_elements(By.XPATH, './div')
                    items.extend(data_items)
            return items, len(data_rows)

        try:
            # --- --- --- Details --- --- ---
            items, n_data_rows = collect_items('//*[@id="details"]')
            n_items = len(items)
            print(f"\n1. Details Tab: {n_items} from {n_data_rows} data rows.")  # if PRINT else None #print(f"Details Tab: {n_items}")

            contact_dict = {'Category': ['Agent', 'Officer']}
            for i, item in enumerate(items):
                item_name = item.find_element(By.XPATH, './label').text.strip()
                data_name = self.details_dict[item_name]
                try:
                    item_value = item.find_element(By.XPATH, './div/textarea').text.strip()
                except NoSuchElementException:
                    item_value = item.find_element(By.XPATH, './div/input').get_attribute('value').strip()
                # print(i, item_name, item_value, type(item_name))
                # if data_name in self.app_dfs.columns:
                try:
                    if data_name in ['Phone', 'Email']:
                        contact_dict[data_name] = [item_value]
                        print(f"    <{item_name}> scraped: {item_value}") if PRINT else None
                    else:
                        app_df.at[data_name] = item_value
                        print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                # New
                except KeyError:
                    app_df[data_name] = item_value
                    print(f"    <{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None

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
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return
                # print('--- --- test --- ---'
        tabs = driver.find_elements(By.XPATH, '//*[@id="ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField"]/div/div[5]/ul/li')
        app_df = self.set_default_items(app_df)

        for tab_index, tab in enumerate(tabs[1:]):
            tab.click()
            tab_name = tab.text.strip()
            # --- --- --- Other Details --- --- ---
            if 'Other Details' in tab_name:
                def parse_other_details():
                    items, n_data_rows = collect_items('//*[@id="otherdetails"]')
                    n_items = len(items)
                    print(f"\n{tab_index+2}. Other Details Tab: {n_items} from {n_data_rows} data rows.") if PRINT else None

                    for i, item in enumerate(items):
                        item_name = item.find_element(By.XPATH, './label').text.strip()
                        data_name = self.details_dict[item_name]
                        try:
                            item_value = item.find_element(By.XPATH, './div/textarea').text.strip()
                        except NoSuchElementException:
                            item_value = item.find_element(By.XPATH, './div/input').get_attribute('value').strip()
                        # print(i, item_name, data_name, item_value, type(item_name)) if PRINT else None
                        # if data_name in self.app_dfs.columns:
                        try:
                            if data_name in ['Phone', 'Email']:
                                contact_dict[data_name].append(item_value)
                                print(f"    <{item_name}> scraped: {item_value}") if PRINT else None
                            elif item_name in ['Environmental Statement', 'Departure']:
                                app_df[data_name] = item_value.find_element(By.XPATH, './input').is_selected()
                                print(f"<{item_name}> scraped (checkbox): {app_df.at[data_name]}") if PRINT else None
                            else:
                                app_df.at[data_name] = item_value
                                print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                        # New
                        except KeyError:
                            app_df[data_name] = item_value
                            print(f"<{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
                    #print(contact_dict)
                    contact_df = pd.DataFrame(contact_dict)
                    contact_df.to_csv(f"{self.data_storage_path}{folder_name}/contact.csv", index=False)
                parse_other_details()
            # --- --- --- Decision --- --- ---
            elif 'Decision' in tab_name:
                def parse_decision():
                    try:
                        driver.find_element(By.XPATH, '//*[@id="decision"]/strong')
                        has_condition = False
                    except NoSuchElementException:
                        has_condition = True

                    items, n_data_rows = collect_items('//*[@id="decision"]', skip_condition=has_condition)
                    n_items = len(items)
                    print(f"\n{tab_index+2}. Decision Tab: {n_items} from {n_data_rows} data rows.") if PRINT else None

                    for i, item in enumerate(items):
                        try:
                            item_name = item.find_element(By.XPATH, './label').text.strip()
                        except NoSuchElementException:
                            continue  # Appeal reference.
                        data_name = self.details_dict[item_name]
                        try:
                            item_value = item.find_element(By.XPATH, './div/textarea').text.strip()
                        except NoSuchElementException:
                            item_value = item.find_element(By.XPATH, './div/input').get_attribute('value').strip()
                        # print(i, item_name, item_value, type(item_name))
                        # if data_name in self.app_dfs.columns:
                        try:
                            app_df.at[data_name] = item_value
                            print(f"<{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                        # New
                        except KeyError:
                            app_df[data_name] = item_value
                            print(f"<{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None

                    # get conditions:
                    if has_condition:
                        condition_item_list = driver.find_element(By.XPATH, '//*[@id="decision"]/div[2]/div[3]')
                        condition_items = condition_item_list.find_elements(By.XPATH, './div')
                        print(f"Condtion items: {len(condition_items)}.") if PRINT else None
                        """
                        condition_dict = {'Condition Details': [condition_item.find_element(By.XPATH, './div[2]').text.strip() for condition_item in condition_items]}
                        condition_df = pd.DataFrame(condition_dict)
                        condition_df.to_csv(f"{self.data_storage_path}{folder_name}/condition_details.csv", index=False)
                        #"""
                        self.scrape_for_csv_single(csv_name='condition_details', column_name='Condition Details', table_items=condition_items,
                                                   folder_name=folder_name, path='div[2]')
                    else:
                        condition_details = driver.find_element(By.XPATH, '//*[@id="decision"]/strong').text.strip()
                        print(condition_details) if PRINT else None
                        assert 'No Conditions Found' in condition_details
                parse_decision()
            # --- --- --- Consultees --- --- ---
            elif 'Consultee' in tab_name:  # Adaptive
                def parse_consultees():
                    try:
                        consultees_table = driver.find_element(By.XPATH, '//*[@id="consultees"]/div')
                        table_items = consultees_table.find_elements(By.XPATH, './div[2]/div')
                        n_items = len(table_items)
                        print(f"\n{tab_index+2}. Consultees Tab: {n_items}") if PRINT else None
                        if n_items > 0:
                            app_df.at['other_fields.n_comments_consultee_responded'] = n_items
                            app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at[
                                'other_fields.n_comments_consultee_responded']
                            table_columns = consultees_table.find_elements(By.XPATH, './div[1]/div')
                            self.scrape_for_csv(csv_name='consultee comments', table_columns=table_columns, table_items=table_items,
                                                folder_name=folder_name, path='div')
                    except NoSuchElementException:
                        print(f"\n{tab_index+2}. " + driver.find_element(By.XPATH, '//*[@id="consultees"]/strong').text.strip())  # No Consultations found for this Application
                parse_consultees()
            # --- --- --- Neighbours --- --- ---
            elif 'Neighbour' in tab_name:  # Adaptive * need tests
                def parse_neighbour():
                    try:
                        neighbours_table = driver.find_element(By.XPATH, '//*[@id="neighbours"]/div')
                        table_items = neighbours_table.find_elements(By.XPATH, './div[2]/div')
                        n_items = len(table_items)
                        print(f"\n{tab_index+2}. Neighbours Tab: {n_items}") if PRINT else None
                        if n_items > 0:
                            app_df.at['other_fields.n_comments_public_received'] = n_items
                            app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at[
                                'other_fields.n_comments_consultee_responded']
                            table_columns = neighbours_table.find_elements(By.XPATH, './div[1]/div')
                            self.scrape_for_csv(csv_name='neighbour comments', table_columns=table_columns, table_items=table_items,
                                                folder_name=folder_name, path='div')
                    except NoSuchElementException:
                        print(f"\n{tab_index+2}. " + driver.find_element(By.XPATH, '//*[@id="neighbours"]/strong').text.strip())  # No Consultations found for this Application
                parse_neighbour()
            # --- --- --- Documents --- --- ---
            elif 'Document' in tab_name:
                [n_documents, file_urls, document_names, app_df] = self.get_documents_from_single_table(driver, app_df, folder_name, tab_index, table_path='//*[@id="documents"]/div[2]/table/tbody', description_path='./td[1]/label/span/a')
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            # --- --- --- Public Notice --- --- ---
            elif 'Public Notice' in tab_name:  # Adaptive
                def parse_public_notice():
                    try:
                        public_notices_table = driver.find_element(By.XPATH, '//*[@id="publicnotices"]/div')
                        table_items = public_notices_table.find_elements(By.XPATH, './div[2]/div')
                        n_items = len(table_items)
                        print(f"\n{tab_index+2}. Public Notices Tab: {n_items}") if PRINT else None
                        if n_items > 0:
                            app_df.at['other_fields.n_public_notices'] = n_items
                            table_columns = public_notices_table.find_elements(By.XPATH, './div[1]/div')
                            self.scrape_for_csv(csv_name='public notice', table_columns=table_columns, table_items=table_items,
                                                folder_name=folder_name, path='div')
                    except NoSuchElementException:
                        print(f"\n{tab_index+2}. " + driver.find_element(By.XPATH, '//*[@id="publicnotices"]/strong').text.strip())  # No Public Notices found for this Application
                parse_public_notice()
            else:
                print(f'Unknown tab: {tab_name}')
                assert 0 == 1
        self.ending(app_df)



    """ Cherwell, WestNorthamptonshire
    Features: [Main Details], [Applicant/Agents], [Publicity], [Supporting Docs], [Properties], [Site History]
              [get_document_info_columns]
        1. (0/2): Need run script to split items and values
        Tab Main Details: Framework {item and value: tbody/tr/td}, need run script to split items and values.
        
        2. Encapsulated(3/3): [Publicity(Neighbours, Consultee, Public Notice), Properties, Site History]
        Tab Publicity: empty table. 
        
        3. Encapsulated Doc system: Multi-tables. 
            #Shared Columns
            #Table1
            #    Document items [date, type(with links), description].
            #Table2
            #    Document items [date, type(with links), description].
    """
    def parse_data_item_B(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        ### Ensure the page content is loaded. Same as the one in parse_data_item_C().
        try:
            try:
                driver.find_element(By.XPATH, '//*[@id="cookie-alert"]/span/button').click()
            except (NoSuchElementException, ElementNotInteractableException):
                pass
            if scraper_name[0] == 'C':
                if 'Agree' in response.xpath('//*[@id="agreeToDisclaimer"]/span[1]/text()').get():
                    print('Click: Agree the Disclaimer/ Copyright / Personal Data - Terms and Conditions.')
                    driver.find_element(By.XPATH, '//*[@id="agreeToDisclaimer"]').click()
            else:  # WestNorthamptonshire
                if 'Disclaim' in response.xpath('//*[@id="main"]/h1/text()').get():
                    print('Click: Agree the Disclaimer/ Copyright / Personal Data - Terms and Conditions.')
                    driver.find_element(By.XPATH, '//*[@id="main"]/form/div/input').click()
        except TypeError:
            pass

        print(f"parse_data_item_B, scraper name: {scraper_name}")
        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)

        # for tabs 'Main Details' and 'Applicant/Agents'
        def scrape_tab_data(tab_panel, tab_index=-1, tab_name='Main Details', scraper_name='Cherwell'):
            if scraper_name[0] == 'C':   # Cherwell
                table_wrappers = tab_panel.find_elements(By.XPATH, './div')
                items = []
                for tab_wrapper in table_wrappers:
                    trs = tab_wrapper.find_elements(By.XPATH, './div/table/tbody/tr')
                    for tr in trs:
                        tds = tr.find_elements(By.XPATH, './td')
                        for item in tds:
                            items.append(item)
            else:  # WestNorthamptonshire
                items = tab_panel.find_elements(By.XPATH, './table/tbody/tr/td')

            n_items = len(items)
            print(f"\n{tab_index+2}. {tab_name} Tab: {n_items}")  # if PRINT else None #print(f"Main Details Tab: {n_items}")
            for i, item in enumerate(items):
                item_name = driver.execute_script("return arguments[0].firstChild.textContent;", item).strip()
                data_name = self.details_dict[item_name]
                item_value = item.find_element(By.XPATH, './div/span').text.strip()
                # print(i, item_name, item_value, type(item_name))
                # if data_name in self.app_dfs.columns:
                try:
                    app_df.at[data_name] = item_value
                    print(f"<{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                # New or contact
                except KeyError:
                    app_df[data_name] = item_value
                    print(f"<{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None

        try:
            # --- --- --- Main Details --- --- ---
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="Main Details"]')))
            scrape_tab_data(tab_panel, scraper_name=scraper_name)
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
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return
                # print('--- --- test --- ---')

        tabs = driver.find_element(By.XPATH, '//*[@id="myTopnav"]').find_elements(By.XPATH, './a')[:-1]  # Different from parse_C func, the last tab in parse_B is useless.
        app_df = self.set_default_items(app_df)
        for tab_index, tab in enumerate(tabs[1:]):
            tab.click()
            tab_name = tab.text.strip()
            # --- --- --- Applicant --- --- ---
            if 'Applicant' in tab_name:
                def parse_applicant():
                    tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="Applicant/ Agents"]')))
                    scrape_tab_data(tab_panel, tab_index=tab_index, tab_name=tab_name, scraper_name=scraper_name)
                parse_applicant()
            # --- --- --- Publicity --- --- ---
            elif 'Publicity' in tab_name:  # scrape_multi_tables_for_csv, empty table.
                def parse_publicity():
                    # check tables: neighbour list, consultee list, public notices ...
                    publicity_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="Publicity"]')))
                    publicity_table_names = publicity_table_list.find_elements(By.XPATH, './strong')
                    table_path = './div/div/table' if scraper_name[0] == 'C' else './table'
                    publicity_tables = publicity_table_list.find_elements(By.XPATH, table_path)
                    n_publicity_tables = len(publicity_tables)
                    print(f"\n{tab_index+2}. Publicity Tab: {n_publicity_tables} tables.") if PRINT else None
                    table_name_dict = {'Neighbour List': 'neighbour comments',
                                        'Consultee List': 'consultee comments',
                                        'Public Notices': 'public notices'}
                    csv_names = [table_name_dict[table_name.text.strip()] for table_name in publicity_table_names]
                    n_table_items = self.scrape_multi_tables_for_csv(csv_names, publicity_tables, folder_name, table_path='tbody/tr', column_path='th', item_path='td')

                    for csv_name, n_items in zip(csv_names, n_table_items):
                        if csv_name == 'neighbour comments':
                            app_df.at['other_fields.n_comments_public_received'] = n_items
                        elif csv_name == 'consultee comments':
                            app_df.at['other_fields.n_comments_consultee_responded'] = n_items
                        elif csv_name == 'public notices':
                            app_df.at['other_fields.n_public_notices'] = n_items
                    app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at['other_fields.n_comments_consultee_responded']
                    print(f"number of comments: {app_df.at['other_fields.n_comments']}")
                parse_publicity()
            # --- --- --- Documents --- --- ---
            elif 'Supporting Docs' in tab_name:  # old identity.
                def get_supporting_documents():
                    """  https://planningregister.cherwell.gov.uk/Planning/Display/11/00006/F#undefined
                    div
                    |-- p "To view ..."
                    |-- div [doc-table-desktop]
                    |   |-- div -- div [docTable]
                    |   |    |---- div [table_wrapper]
                    |   |           |-- div [scroller]
                    |   |                |--table -- thead -- tr [header]---- th -- label [select All]
                    |   |                     |                 |               |--- input [select All]
                    |   |                     |                 |
                    |   |                     |                 |-- ths [Document Type/ Date/ Description/ File Size]
                    |   |                     |
                    |   |                     |----- tbody [* each tbody is a doc table *] -- tr [table category name]
                    |   |                     |        |------------------------------------- trs [documents] -- td [checkbox]
                    |   |                     |                                                |---------------- tds [item info]
                    |   |                     |
                    |   |                     |----- tbody
                    |   |                     |----- ...
                    |   |                     |----- tbody
                    |   |
                    |   |--button  [Start Download]
                    |--div
                    """
                    try:
                        table_path = '//*[@id="Documents"]/div[1]/div/div[2]/div/table' if scraper_name[0] == 'C' else '//*[@id="Documents"]/div[1]/div/table'
                        document_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, table_path)))
                    except TimeoutException:
                        app_df.at['other_fields.n_documents'] = 0
                        print(f"\n{tab_index+2}. <NULL> Document Tab: {app_df.at['other_fields.n_documents']} items.") if PRINT else None
                        return 0, [], []

                    document_tables = document_table_list.find_elements(By.XPATH, './tbody')
                    n_tables = len(document_tables)
                    print(f"\n{tab_index+2}. Document Tab: {n_tables} tables") if PRINT else None

                    columns = document_table_list.find_elements(By.XPATH, './thead/tr/th')
                    [date_column, type_column, description_column] = self.get_column_indexes(columns, keywords=['date', 'type', 'description'])

                    n_documents, file_urls, document_names = 0, [], []
                    for table_index, document_table in enumerate(document_tables):
                        document_table_name = document_table.find_element(By.XPATH, './tr[1]/th').text.strip()
                        document_items = document_table.find_elements(By.XPATH, './tr')[1:]
                        n_table_documents = len(document_items)
                        print(f"Table {table_index+1}: {document_table_name}, including {n_table_documents} documents.") if PRINT else None
                        for document_item in document_items:
                            n_documents += 1
                            file_url = document_item.find_element(By.XPATH, f'./td[{type_column}]/a').get_attribute('href')
                            file_urls.append(file_url)

                            item_identity = file_url.split('=')[-1]  # includes extension such as .pdf
                            document_name = f"uid={item_identity}"
                            document_name = self.rename_document(document_item, document_name, description_column, type_column, date_column, path='td')

                            #document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
                            print(f"    Document {n_documents}: {document_name}") if PRINT else None
                            document_name = replace_invalid_characters(document_name)
                            document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                    app_df.at['other_fields.n_documents'] = n_documents
                    print(f'Total documents: {n_documents}') if PRINT else None
                    return n_documents, file_urls, document_names
                n_documents, file_urls, document_names = get_supporting_documents()
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            # --- --- --- Properties --- --- ---
            elif 'Properties' in tab_name: # scrape_for_csv_single, empty table.
                def parse_properties():
                    table_path = '//*[@id="Addresses"]/div/div/table/tbody' if scraper_name[0] == 'C' else '//*[@id="Addresses"]/table/tbody'
                    property_table = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, table_path)))

                    column_name = property_table.find_element(By.XPATH, './tr[1]/th').text.strip()
                    property_items = property_table.find_elements(By.XPATH, './tr')[1:]
                    print(f"\n{tab_index+2}. Properties Tab: {column_name} with {len(property_items)} items.") if PRINT else None
                    if len(property_items)>0:
                        self.scrape_for_csv_single(csv_name='properties', column_name=column_name, table_items=property_items,
                                                   folder_name=folder_name, path='td')
                parse_properties()
            # --- --- --- Site History --- --- ---
            elif 'Site History' in tab_name: # scrape_multi_tables_for_csv_inner_tablename_with_prefix
                def parse_site_history():
                    try:
                        site_history_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="RelatedCases"]')))
                    except TimeoutException:
                        print(f"\n{tab_index+2}. Site History Tab <NULL>.") if PRINT else None
                        return
                    table_path = './div/div/table' if scraper_name[0] == 'C' else './table'
                    site_history_tables = site_history_table_list.find_elements(By.XPATH, table_path)
                    n_site_history_tables = len(site_history_tables)
                    print(f"\n{tab_index+2}. Site History Tab: {n_site_history_tables} tables.") if PRINT else None

                    self.scrape_multi_tables_for_csv_inner_tablename_with_prefix('site history_', site_history_tables, folder_name,
                                                                                 table_path='tbody/tr', name_path='td/h2', column_path='th', item_path='td')
                parse_site_history()
            else:
                print('Unknown tab.')
                assert 0 == 1
        self.ending(app_df)


    """ Crawley | try empty x 1
    https://planningregister.crawley.gov.uk/Planning/Display/CR/2004/0007/FUL
    Features: [Main Details], [Applicant], [Publicity], [Supporting Documents]
              [get_document_info_columns]
        1. Encapsulated(2/2)
        Tab Main Details:   Framework {item: label, value: div/span}
                            Has contacts (Phone, Email). 
                            
        2. Encapsulated(1/1): [Publicity(Neighbours, Consultee, Public Notice)]
        
        3. Encapsulated Doc system <get_documents_from_single_table>: 
        Single table with headers and document items mixed. Framework [date, description(with links)].
        * update on 24-11-09: Old implementation used url ids as doc ids, and doc names have not type info. New implementation uses n_doc as doc ids, add type.
    """
    def parse_data_item_C(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        ### Ensure the page content is loaded. Same as the one in parse_data_item_B().
        try:
            try:
                driver.find_element(By.XPATH, '//*[@id="cookie-alert"]/span/button').click()
            except (NoSuchElementException, ElementNotInteractableException):
                pass
            if 'Agree' in response.xpath('//*[@id="agreeToDisclaimer"]/span[1]/text()').get():
                print('Click: Agree the Disclaimer/ Copyright / Personal Data - Terms and Conditions.')
                driver.find_element(By.XPATH, '//*[@id="agreeToDisclaimer"]').click()
        except TypeError:
            pass
        print("parse_data_item_C")

        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)

        try:
            # --- --- --- Main Details --- --- ---
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="proposal"]')))

            items = tab_panel.find_elements(By.XPATH, './div/label')
            item_values = tab_panel.find_elements(By.XPATH, './div/div/span')
            n_items = len(items)
            print(f"\n1. Main Details Tab: {n_items}")  # if PRINT else None #print(f"Main Details Tab: {n_items}")
            app_df = self.scrape_data_items_including_contacts(app_df, items, item_values, folder_name)
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
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return
                # print('--- --- test --- ---')

        """
        selection_input = driver.find_element(By.XPATH, '//*[@id="topOfContent"]/div[6]/select')
        selection_options = selection_input.find_elements(By.XPATH, './option')
        option_values = []
        for option in selection_options:
            option_values.append(option.text.strip())
        print(f"Number of tabs: {len(selection_options)}, options: {option_values}") if PRINT else None
        #"""

        tabs = driver.find_element(By.XPATH, '//*[@id="topOfContent"]/ul').find_elements(By.XPATH, './li')
        app_df = self.set_default_items(app_df)
        for tab_index, tab in enumerate(tabs[1:]):
            tab.find_element(By.XPATH, './a').click()
            tab_name = tab.find_element(By.XPATH, './a').text.strip()
            # --- --- --- Applicant --- --- ---
            if 'Applicant' in tab_name:  # scrape_data_items
                tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="applicant"]')))
                items = tab_panel.find_elements(By.XPATH, './div/label')
                item_values = tab_panel.find_elements(By.XPATH, './div/div/span')
                n_items = len(items)
                print(f"\n{tab_index+2}. Applicant Tab: {n_items}") if PRINT else None
                app_df = self.scrape_data_items(app_df, items, item_values)
            # --- --- --- Publicity --- --- ---
            elif 'Publicity' in tab_name:  # scrape_multi_tables_for_csv_inner_tablename. No empty table.
                def parse_publicity():
                    """ https://planningregister.crawley.gov.uk/Planning/Display/CR/2012/0009/FUL#PublicityTab
                       Address, Date Letter Sent
                       div
                       |--div--label [neighbour list]
                       |--div--divs [columns]
                       |--div--divs [items] -- div [Address]
                                             |--div [Date Letter Sent]
                    """
                    publicity_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="consultees"]')))
                    #publicity_table_names = publicity_table_list.find_elements(By.XPATH, './strong')
                    publicity_tables = publicity_table_list.find_elements(By.XPATH, './div')[1:]  # the first div are texts
                    n_publicity_tables = len(publicity_tables)
                    print(f"\n{tab_index+2}. Publicity Tab: {n_publicity_tables} tables.") if PRINT else None
                    table_name_dict = {'neighbour list': 'neighbour comments',
                                        'consultee list': 'consultee comments',
                                        'public notices': 'public notices'}
                    n_table_items, csv_names = self.scrape_multi_tables_for_csv_inner_tablename(table_name_dict, publicity_tables, folder_name,
                                                                                     table_path='div', name_path='label', column_path='div', item_path='div', pre_item_path='div')
                    for csv_name, n_items in zip(csv_names, n_table_items):
                        if csv_name == 'neighbour comments':
                            app_df.at['other_fields.n_comments_public_received'] = n_items
                        elif csv_name == 'consultee comments':
                            app_df.at['other_fields.n_comments_consultee_responded'] = n_items
                        elif csv_name == 'public notices':
                            app_df.at['other_fields.n_public_notices'] = n_items
                    app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at['other_fields.n_comments_consultee_responded']
                    print(f"number of comments: {app_df.at['other_fields.n_comments']}")
                parse_publicity()
            # --- --- --- Documents --- --- --- for a given documents framework, its different document tables share the same columns.
            elif 'Supporting Documents' in tab_name:  # Specific, no adaptive columns  [description(name:Select all documents) with links]
                """  https://planningregister.crawley.gov.uk/Planning/Display/CR/2012/0009/FUL#SupportingDocumentsTab
                div
                |--div
                    |-- p "To view ..."
                    |--div  [No margin] -- div -- div [Select all documents] -- label -- span
                    |                       |---- div [Size]
                    |                       |---- div [Date]
                    |--div -- table -- tbody -- tr [header] -- th -- span
                    |                    |----- trs [items] -- td -- label -- span -- a [url]
                    |                            |------------ tds [other items]
                    |                    |----- tr [header] -- ...
                    |                    |----- trs [items] -- ...
                    |                    |----- ...
                    |--div  [btn-group, start download button]
                    |--p  "To download ..."
                """
                [n_documents, file_urls, document_names, app_df] = self.get_documents_from_single_table(driver, app_df, folder_name, tab_index, table_path='//*[@id="documents"]/div/div[2]/table/tbody', description_path='./td[1]/label/span/a')
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            else:
                print('Unknown tab.')
                assert 0 == 1
        self.ending(app_df)


    """ Cumbria, https://planning.cumbria.gov.uk/Planning/Display/5/01/9006#undefined
    Features:  Mage Page | Multiple Document Tabs.  
        1. Encapsulated(1/1)
        Tab Main Page: Framework {item: dt, value: dd}
        
        2. (0/0): No tab for other consultations, constraints, details.
        
        3. Encapsulated Doc system: Multiple tabs for documents.
    """
    def parse_data_item_Cumbria(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        ### Ensure the page content is loaded.
        try:
            # If there is a dialog asking for subscription.
            if 'No thanks' in response.xpath('//*[@id="prefix-dismissButton"]/text()').get():
                print('--- --- No Thanks --- ---')
                driver.find_element(By.XPATH, '//*[@id="prefix-dismissButton"]').click()

            # If there is a dialog asking for cookie preference.
            try:
                driver.find_element(By.XPATH, '//*[@id="cookie-bar"]/p/a[1]').click()
                #WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="cookie-bar"]/p/a[1]'))).click()
            except (NoSuchElementException, ElementNotInteractableException, ElementClickInterceptedException):
                pass

            if 'Terms of Use' in response.xpath('//*[@id="home-page-content"]/div/div/div/div/div/div/h1/text()').get():
                print('Click: Accept Terms of Use.')
                driver.find_element(By.XPATH, '//*[@id="DisclaimerButton"]').click()
        except TypeError:
            pass
        print("parse_data_item_Cumbria")

        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)

        try:
            # --- --- --- Main Details --- --- ---
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="MainDetails"]/dl')))
            items = tab_panel.find_elements(By.XPATH, './dt')
            item_values = tab_panel.find_elements(By.XPATH, './dd')
            n_items = len(items)
            print(f"\n1. Details Tab: {n_items}") if PRINT else None #print(f"Details Tab: {n_items}")
            app_df = self.scrape_data_items(app_df, items, item_values)
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
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return
                # print('--- --- test --- ---')

        doc_tabs = driver.find_elements(By.XPATH, '//*[@id="myTopnav"]/a')[:-1]
        app_df = self.set_default_items(app_df)

        n_documents, file_urls, document_names = 0, [], []
        for tab_index, tab in enumerate(doc_tabs):
            tab.click()
            tab_name = tab.text.strip()
            def get_supporting_documents_from_tab(tab_index, tab_name, n_documents, file_urls, document_names):
                document_table = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@id="tab-content"]/div[{tab_index+1}]/table/tbody')))
                document_items = document_table.find_elements(By.XPATH, './tr')
                n_table_documents = len(document_items)
                print(f"\nDocument Tab {tab_index+1}: <{tab_name}>, including {n_table_documents} documents.") if PRINT else None
                for document_item in document_items:
                    n_documents += 1
                    file_url = document_item.find_element(By.XPATH, f'./td[2]/a').get_attribute('href')
                    file_urls.append(file_url)

                    item_identity = file_url.split('.')[-1]  # uid is included in doc description, so identity contains only an extension such as .pdf
                    document_name = f".{item_identity}"
                    document_name = self.rename_document_date_desc(document_item, document_name, document_type=tab_name,
                                                                   description_column=2, date_column=3, path='td')
                    """
                    try:
                        document_description = document_item.find_element(By.XPATH, f'./td[2]').text.strip()
                        if len(document_description) > 0:
                            document_name = f"desc={document_description}&{document_name}"
                    except NoSuchElementException:
                        pass
                    # Use doc tab name as doc type name
                    document_type = tab_name # driver.find_element(By.XPATH, f'//*[@id="myTopnav"]/a[{tab_index+1}]').text.strip()
                    document_name = f"type={document_type}&{document_name}"
                    try:
                        document_date = document_item.find_element(By.XPATH, f'./td[3]').text.strip()
                        if len(document_date) > 0:
                            document_name = f"date={document_date}&{document_name}"
                    except NoSuchElementException:
                        pass
                    #"""
                    # document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
                    print(f"    Document {n_documents}: {document_name}") if PRINT else None
                    document_name = replace_invalid_characters(document_name)
                    document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                return n_documents, file_urls, document_names
            n_documents, file_urls, document_names = get_supporting_documents_from_tab(tab_index, tab_name, n_documents, file_urls, document_names)
        app_df.at['other_fields.n_documents'] = n_documents
        print(f'Total documents: {n_documents}') if PRINT else None
        if n_documents > 0:
            item = self.create_item(driver, folder_name, file_urls, document_names)
            yield item
        self.ending(app_df)


    """ Essex, https://planning.essex.gov.uk/Planning/Display/ESS/01/03/HLW  #***# Appeal details
    Features: [Main Details], [Location], [Map], [Associated Documents], [Consultees], [Appeal Details]
        Need tab_index for parse_funcs.
        
        1. Encapsulated(2/2)
        Tab Main Details: Framework {item: dt, value: dd}
        
        2. Encapsulated(0.5/1) [Consultees]
        Tab Consultees has only consultation comments, encapsulated by self.scrape_for_csv.
        column path: './thead/tr/th'
        
        3. Encapsulated Doc system: Multi-tables with types as sub table names. [Similar to Glamorgan except column names] 
            #Shared Columns
            #Type1
            #    Document items [date, description(with links)].
            #Type2
            #    Document items [date, description(with links)].
    """
    def parse_data_item_Essex(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        ### Ensure the page content is loaded.
        try:
            if 'Disclaimer' in response.xpath('//*[@id="main"]/div/div[1]/div/h1/text()').get():
                print('Click: Agree the Copyright & disclaimer.')
                driver.find_element(By.XPATH, '//*[@id="topOfContent"]/form/div/input').click()
        except TypeError:
            pass
        print(f"parse_data_item_Essex, scraper name: {scraper_name}")

        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)
        try:
            # --- --- --- Main Details --- --- ---
            data_table = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="tab-content"]/div[1]')))
            items = data_table.find_elements(By.XPATH, './dl/div/dt')
            item_values = data_table.find_elements(By.XPATH, './dl/div/dd/span')
            n_items = len(items)
            print(f"\n1. Main Details Tab: {n_items}")  # if PRINT else None #print(f"Details Tab: {n_items}")
            app_df = self.scrape_data_items(app_df, items, item_values)
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
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return
                # print('--- --- test --- ---')

        tabs = driver.find_element(By.XPATH, '//*[@id="myTopnav"]').find_elements(By.XPATH, './a')[:-1]
        app_df = self.set_default_items(app_df)

        for tab_index, tab in enumerate(tabs[1:]):
            tab.click()
            tab_name = tab.text.strip()
            # --- --- --- Location --- --- ---
            if 'Location' in tab_name:
                data_table = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@id="tab-content"]/div[{tab_index+2}]')))
                items = data_table.find_elements(By.XPATH, './dl/div/dt')
                item_values = data_table.find_elements(By.XPATH, './dl/div/dd/span')
                n_items = len(items)
                print(f"\n{tab_index+2}. Location Tab: {n_items}") if PRINT else None
                app_df = self.scrape_data_items(app_df, items, item_values)
            # --- --- --- Associated Documents --- --- ---
            elif 'Associated Documents' in tab_name:
                def get_documents(tab_index):
                    """
                    table:
                        thead:
                            tr:
                                th: <column: checkbox>
                                th: <column: Description>
                                th: <column: Date Uploaded>
                        tbody
                            tr:
                                th: <subtable title: Type>
                            tr <item>:
                                td: span <checkbox>
                                td:
                                    <a: file link>: Description info.
                                td: Date info
                            tr <item>: ...
                    """
                    try:
                        document_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@id="tab-content"]/div[{tab_index+2}]/div/table')))
                    except TimeoutException:
                        app_df.at['other_fields.n_documents'] = 0
                        print(f"\n{tab_index+2}. <NULL> Document Tab: {app_df.at['other_fields.n_documents']} items.") if PRINT else None
                        return 0, [], []

                    document_tables = document_table_list.find_elements(By.XPATH, './tbody')
                    n_tables = len(document_tables)
                    print(f"\n{tab_index+2}. Document Tab: {n_tables} tables") if PRINT else None

                    columns = document_table_list.find_elements(By.XPATH, './thead/tr/th')
                    [date_column, description_column] = self.get_column_indexes(columns, keywords=['date', 'description'])

                    n_documents, file_urls, document_names = 0, [], []
                    for table_index, document_table in enumerate(document_tables):
                        document_table_name = document_table.find_element(By.XPATH, './tr[1]/th').text.strip()
                        document_items = document_table.find_elements(By.XPATH, './tr')[1:]
                        n_table_documents = len(document_items)
                        print(f"Table {table_index+1}: {document_table_name}, including {n_table_documents} documents.") if PRINT else None
                        for document_item in document_items:
                            n_documents += 1
                            file_url = document_item.find_element(By.XPATH, f'./td[{description_column}]/a').get_attribute('href')
                            file_urls.append(file_url)

                            item_extension = file_url.split('.')[-1]
                            document_name = f"uid={n_documents}.{item_extension}"
                            document_name = self.rename_document_date_desc(document_item, document_name, document_type=document_table_name,
                                                                           description_column=description_column, date_column=date_column, path='td')
                            # document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_extension}"
                            print(f"    Document {n_documents}: {document_name}") if PRINT else None
                            document_name = replace_invalid_characters(document_name)
                            document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                    app_df.at['other_fields.n_documents'] = n_documents
                    print(f'Total documents: {n_documents}') if PRINT else None
                    return n_documents, file_urls, document_names
                n_documents, file_urls, document_names = get_documents(tab_index)
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            # --- --- --- Consultatees --- --- ---
            elif 'Consultee' in tab_name:  # Adaptive columns. *Only for consultee comments, no public/neighbour comments or constraints. An assert statement is added to detect other comments.*
                def parse_consultees(tab_index):
                    consultation_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@id="tab-content"]/div[{tab_index+2}]')))
                    consultation_tables = consultation_table_list.find_elements(By.XPATH, './div/table')
                    n_consultation_tables = len(consultation_tables)
                    print(f"\n{tab_index+2}. Consultees Tab, {n_consultation_tables} tables") if PRINT else None
                    assert n_consultation_tables <= 1  # (0 or 1)

                    def scrape_consultations(table_columns, table_items):
                        self.scrape_for_csv(csv_name='consultee comments', table_columns=table_columns, table_items=table_items, folder_name=folder_name, path='td')
                        """
                        if 'neighbour' in table_name:
                            app_df.at['other_fields.n_comments_public_received'] = len(table_items)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/neighbour comments.csv", index=False)
                        elif 'consult' in table_name:
                            app_df.at['other_fields.n_comments_consultee_responded'] = len(table_items)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/consultee comments.csv", index=False)
                        elif 'constraint' in table_name:
                            app_df.at['other_fields.n_constraints'] = len(table_items)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/constraints.csv", index=False)
                        else:
                            self.is_empty(app_df.at[table_name])  # ***** to be validate, throw out an error when having more tables.
                        """
                        app_df.at['other_fields.n_comments_consultee_responded'] = len(table_items)

                    # scrape each table:
                    for table_index, consultation_table in enumerate(consultation_tables):
                        #consultation_table_name = consultation_names[table_index]
                        consultation_table_columns = consultation_table.find_elements(By.XPATH, './thead/tr/th')
                        consultation_table_items = consultation_table.find_elements(By.XPATH, './tbody/tr')

                        if len(consultation_table_columns) > 0 and len(consultation_table_items) > 0:
                            scrape_consultations(consultation_table_columns, consultation_table_items)
                        else:  # ***** To be validate
                            print(f"NULL Table") if PRINT else None
                    app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at['other_fields.n_comments_consultee_responded']
                    print(f"number of comments: {app_df.at['other_fields.n_comments']}")
                parse_consultees(tab_index)
            # --- --- --- Appeal Details --- --- ---
            elif 'Appeal Details' in tab_name:
                def parse_appeal_details(tab_index):
                    tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@id="tab-content"]/div[{tab_index+2}]')))
                    no_appeals = tab_panel.find_element(By.XPATH, './p').text.strip()
                    print(no_appeals) if PRINT else None
                    assert 'no appeals' in no_appeals
                parse_appeal_details(tab_index)
            elif 'Map' in tab_name:
                pass
            else:
                print('Unknown tab.')
                assert 0 == 1
        self.ending(app_df)


    """ Fylde
    Features: Planning Online Status | [Application Details(Summary/Important Dates/Further Information/ Condition Details//Information Notes)], 
                                        [Documents], ([Consultations]), ([Map]), [Appeals]  #***#  Application Details + Appeals
        Tab Main Details: Framework {item: dd[1], value: dd[2]}
    """
    def parse_data_item_Fylde(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        ### Ensure the page content is loaded.
        try:
            if 'Disclaimer' in response.xpath('//*[@id="pe-maincontent"]/article/div/h2/text()').get():
                print('Click: Agree the disclaimer.')
                driver.find_element(By.XPATH, '//*[@id="pe-maincontent"]/article/div/form/div/input').click()
        except TypeError:
            pass

        scraper_name = app_df.at['scraper_name']
        print(f"parse_data_item_Fylde, scraper name: {scraper_name}")
        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)

        try:
            # --- --- --- Application Details (Summary, Important Dates, Further Information, Condition Details / Information Notes) --- --- ---
            if scraper_name == 'WestmorlandFurness':
                tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="Main Details"]')))
            else:  # Fylde, MalvernHills, NorthDevon, Worcester, Wychavon
                tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="MainDetails"]')))
            tables = tab_panel.find_elements(By.XPATH, './table')
            n_tables = len(tables)
            print(f"\n1. Application Details Tab: {n_tables} tables.")  # if PRINT else None #print(f"Main Details Tab: {n_items}")
            for table_index, table in enumerate(tables):
                items = table.find_elements(By.XPATH, './tbody/tr')
                table_name = items[0].find_element(By.XPATH, './td').text.strip()
                print(f"  Table {table_index+1}: {table_name}") if PRINT else None
                for item_index, item in enumerate(items[1:]):
                    item_name = item.find_element(By.XPATH, './td[1]').text.strip()
                    data_name = self.details_dict[item_name]
                    item_value = item.find_element(By.XPATH, './td[2]').text.strip()
                    #print(f"    {item_index}: {item_name}, {item_value}, {type(item_name)}")
                    # if data_name in self.app_dfs.columns:
                    try:
                        app_df.at[data_name] = item_value
                        print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                    # New
                    except KeyError:
                        app_df[data_name] = item_value
                        print(f"    <{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
            assert app_df.at['other_fields.appeal_result'] == 'N/A'
            """
            Condition Details / Information Notes, 
            i.e. https://planningregister.westmorlandandfurness.gov.uk/Planning/Display/01/0019#undefined
            """
            try:
                condition_details_list = driver.find_elements(By.XPATH, '//*[@id="CRTbl"]/table/tbody/tr')[1:]
                if len(condition_details_list) > 0:
                    condition_dict = {'Condition Details / Information Notes': [item.find_element(By.XPATH, './td').text.strip() for item in condition_details_list]}
                    condition_df = pd.DataFrame(condition_dict)
                    condition_df.to_csv(f"{self.data_storage_path}{folder_name}/condition details & information notes.csv", index=False)
            except NoSuchElementException:
                pass
        except TimeoutException:  #***** situation to be completed: application not available
            # Planning Application details not available . e.g. auth=123, year=[21:]
            note = response.xpath('//*[@id="pageheading"]/h1/text()').get()
            print('note: ', note)
            # This application is no longer available for viewing. It may have been removed or restricted from public viewing.
            if note is not None and 'details not available' in note:
                print('*** *** *** This application is not available. *** *** ***')
                return
            else:
                print('*** *** *** NEED TO RELOAD APP PAGE. *** *** ***')
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return

        def tab_operations_for_Fylde(tab):
            tab.find_element(By.XPATH, './a').click()
            return tab.find_element(By.XPATH, './a/p').text.strip()

        def tab_operations_for_WestmorlandFurness(tab):
            #print(tab)
            # <selenium.webdriver.remote.webelement.WebElement (session="160c9889772456584a9cc7932d2e00d1", element="f.CFA84D3300E5D2862A220E06596037B9.d.EDA9DF7DFEF6F4C73058A1DF096212AE.e.213")>
            print(f"tab name: {tab.text.strip()}")
            tab.click()
            return tab.text.strip()

        tab_operation = tab_operations_for_Fylde
        if scraper_name == 'Fylde':
            tabs = driver.find_elements(By.XPATH, '//*[@id="pe-maincontent"]/article/div/div[3]/div/div')
            #tabs = driver.find_element(By.XPATH, '//*[@id="pe-maincontent"]/article/div/div[3]/div').find_elements(By.XPATH, './div')
        elif scraper_name in ['MalvernHills', 'Worcester', 'Wychavon']:
            tabs = driver.find_elements(By.XPATH, '/html/body/div/div[2]/div/div/div/div[3]/ul[1]/li')
        elif scraper_name == 'NorthDevon':
            tabs = driver.find_elements(By.XPATH, '//*[@id="main"]/div[2]/div[2]/div[5]/ul[1]/li')
        elif scraper_name == 'WestmorlandFurness':
            #WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="myTopnav"]/a[2]'))).click()
            tabs = driver.find_elements(By.XPATH, '//*[@id="myTopnav"]/a')[:-1]
            #tabs = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="myTopnav"]/a')))
            # //*[@id="myTopnav"]/a[5]
            tab_operation = tab_operations_for_WestmorlandFurness
        else:
            tabs = driver.find_elements(By.XPATH, '//*[@id="pe-maincontent"]/article/div/div[3]/div/div')

        app_df = self.set_default_items(app_df)
        for tab_index, tab in enumerate(tabs[1:]):
            #tab.find_element(By.XPATH, './a').click()
            #tab_name = tab.find_element(By.XPATH, './a/p').text.strip()
            tab_name = tab_operation(tab)
            #print(f"tab {tab_index + 1}: {tab_name}")
            # --- --- --- Documents --- --- ---
            if 'Documents' in tab_name:  # Adaptive columns, link with doc type, multiple tables in different tbodies {Planning Application Documents, Planning Decision Documents}.
                def get_supporting_documents():
                    try:
                        document_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="Documents"]/div/table')))
                    except TimeoutException:
                        app_df.at['other_fields.n_documents'] = 0
                        print(f"\n{tab_index+2}. <NULL> Document Tab: {app_df.at['other_fields.n_documents']} items.") if PRINT else None
                        return 0, [], []

                    document_tables = document_table_list.find_elements(By.XPATH, './tbody')
                    n_tables = len(document_tables)
                    print(f"\n{tab_index+2}. Document Tab: {n_tables} tables") if PRINT else None

                    columns = document_table_list.find_elements(By.XPATH, './thead/tr/th')
                    def get_document_info_columns(columns):
                        n_columns = len(columns)
                        date_column, type_column, description_column = n_columns, n_columns, n_columns
                        for i, column in enumerate(columns):
                            try:
                                if 'date' in str.lower(column.text.strip()):
                                    date_column = i + 1
                                    continue
                                if 'type' in str.lower(column.text.strip()):
                                    type_column = i + 1
                                    continue
                                if 'description' in str.lower(column.text.strip()):
                                    description_column = i + 1
                                    continue
                            except TypeError:
                                continue
                        print(f"    Columns: date column {date_column}/{n_columns}, type column {type_column}/{n_columns}, description column {description_column}/{n_columns}") if PRINT else None
                        return date_column, type_column, description_column
                    date_column, type_column, description_column = get_document_info_columns(columns)

                    n_documents, file_urls, document_names = 0, [], []
                    driver.find_element(By.CLASS_NAME, 'copyright-agreement').click()
                    for table_index, document_table in enumerate(document_tables):
                        document_table_name = document_table.find_element(By.XPATH, './tr[1]/th').text.strip()
                        document_items = document_table.find_elements(By.XPATH, './tr')[1:]
                        n_table_documents = len(document_items)
                        print(f"Table {table_index+1}: {document_table_name}, including {n_table_documents} documents.") if PRINT else None
                        for document_index, document_item in enumerate(document_items):
                            file_url = document_item.find_element(By.XPATH, f'./td[{type_column}]/a').get_attribute('href')
                            file_urls.append(file_url)

                            item_identity = file_url.split('=')[-1]  # includes extension such as .pdf
                            if len(item_identity) > 24:
                                print(f"--- --- --- too long item identity: {item_identity} --- --- ---") if PRINT else None
                                extension = item_identity.split('.')[-1]
                                len_extension = len(extension) + 1  #  add the length of 'dot'.
                                shorten_identity = ''.join(re.findall(r'\d', item_identity[:-len_extension]))
                                item_identity = f"{shorten_identity}.{extension}"
                                print(f"--- --- --- short item identity: {item_identity} --- --- ---") if PRINT else None
                            document_name = f"uid={item_identity}"
                            try:
                                document_description = document_item.find_element(By.XPATH, f'./td[{description_column}]').text.strip()
                                document_name = f"desc={document_description}&{document_name}"
                            except NoSuchElementException:
                                pass
                            document_type = document_item.find_element(By.XPATH, f'./td[{type_column}]').text.strip()
                            document_name = f"type={document_type}&{document_name}"
                            try:
                                document_date = document_item.find_element(By.XPATH, f'./td[{date_column}]').text.strip()
                                document_name = f"date={document_date}&{document_name}"
                            except NoSuchElementException:
                                pass
                            # document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
                            print(f"    Document {document_index+1}: {document_name}") if PRINT else None
                            document_name = replace_invalid_characters(document_name)
                            document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                        n_documents += n_table_documents
                    app_df.at['other_fields.n_documents'] = n_documents
                    print(f'Total documents: {n_documents}') if PRINT else None
                    return n_documents, file_urls, document_names
                n_documents, file_urls, document_names = get_supporting_documents()
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            # --- --- --- Consultations --- --- ---
            elif 'Consultations' in tab_name:  # Adaptive columns, designed for three tables {neighbour, consultee, constraint}.
                def parse_consultations():
                    consultation_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="Consultations"]')))
                    consultation_names = ['neighbour comments', 'consultee comments', 'constraints']

                    """ 
                    # Fylde:
                    h2: list name
                    h3: text (no associated ...) 
                    table/tbody/tr[1]/td: columns
                    table/tbody/tr[1:]/td: items
                    
                    # [MalvernHills, Worcester, Wychavon]: XPATHs; MalvernHills has a Map tab. 
                    h3: list name;
                    table/tbody/tr[1]/td: columns/ text (no associated ...)
                    table/tbody/tr[1:]/td: items
                    
                    # NorthDevon: XPATHs; 
                    h3: list name;
                    table/thead/tr/th: columns/ text (no associated ...)
                    table/tbody/tr/td: items
                    
                    # WestmorlandFurness [***to be completed]
                    h3: list name; 
                    """
                    if scraper_name == 'Fylde':  # Examine h3 for unavailable lists.
                        consultation_unavailable_list = consultation_table_list.find_elements(By.XPATH, './h3')
                        n_unavailable_tables = len(consultation_unavailable_list)
                        if n_unavailable_tables > 0:
                            consultation_unavailable_strings = [unavailable_name.text.strip() for unavailable_name in consultation_unavailable_list]
                            consultation_unavailable_names = []
                            neighbour_flag, consult_flag, constraint_flag = True, True, True
                            for unavailable_index in range(n_unavailable_tables):
                                unavailable_string = consultation_unavailable_strings[unavailable_index]
                                if neighbour_flag and ('neighbour' in unavailable_string):
                                    consultation_unavailable_names.append('neighbour comments')
                                    neighbour_flag = False
                                elif consult_flag and ('consult' in unavailable_string):
                                    consultation_unavailable_names.append('consultee comments')
                                    consult_flag = False
                                elif constraint_flag and ('constraint' in unavailable_string):
                                    consultation_unavailable_names.append('constraints')
                                    constraint_flag = False
                            consultation_names = [table_name for table_name in consultation_names if table_name not in consultation_unavailable_names]

                    consultation_tables = consultation_table_list.find_elements(By.XPATH, './table')
                    n_consultation_tables = len(consultation_tables)
                    print(f"\n{tab_index+2}. Consultations Tab, {n_consultation_tables} tables: {consultation_names}") if PRINT else None

                    def scrape_consultations(table_name, table_columns, table_items):  # Similar to the one in parse_B but different in XPATH: f'./div[{column_index+1}]'
                        content_dict = {}
                        column_names = [column.text.strip() for column in table_columns]
                        print(f'{table_name}, {len(table_items)} items with column names: ', column_names) if PRINT else None
                        n_columns = len(column_names)

                        for column_index in range(n_columns):
                            item_values = []
                            for table_item in table_items:
                                item_values.append(table_item.find_element(By.XPATH, f'./td[{column_index+1}]').text.strip())
                            content_dict[column_names[column_index]] = item_values

                        content_df = pd.DataFrame(content_dict)
                        if 'neighbour' in table_name:
                            app_df.at['other_fields.n_comments_public_received'] = len(table_items)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/neighbour comments.csv", index=False)
                        elif 'consult' in table_name:
                            app_df.at['other_fields.n_comments_consultee_responded'] = len(table_items)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/consultee comments.csv", index=False)
                        elif 'constraint' in table_name:
                            app_df.at['other_fields.n_constraints'] = len(table_items)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/constraints.csv", index=False)
                        else:
                            assert 0 == 1  # throw out an exception for further investigation.
                    # scrape each table:
                    for table_index, consultation_table in enumerate(consultation_tables):
                        consultation_table_name = consultation_names[table_index]
                        if scraper_name in ['NorthDevon'] and 'constraint' not in consultation_table_name.lower():
                            consultation_table_columns = consultation_table.find_elements(By.XPATH, './thead/tr/th')
                            consultation_table_items = consultation_table.find_elements(By.XPATH, './tbody/tr')
                        else:
                        #if scraper_name in ['Fylde', 'MalvernHills', 'Worcester', 'Wychavon']:
                            consultation_table_columns = consultation_table.find_elements(By.XPATH, './tbody/tr[1]/*')
                            consultation_table_items = consultation_table.find_elements(By.XPATH, './tbody/tr')[1:]

                        if len(consultation_table_columns) > 0 and len(consultation_table_items) > 0:
                            scrape_consultations(consultation_table_name, consultation_table_columns, consultation_table_items)
                        else: #***** To be validate
                            print(f"{consultation_table_name} <NULL>") if PRINT else None
                    app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at['other_fields.n_comments_consultee_responded']
                    print(f"number of comments: {app_df.at['other_fields.n_comments']}")
                parse_consultations()
            # --- --- --- Appeals --- --- --- for a given documents framework, its different document tables share the same columns.
            elif 'Appeals' in tab_name:  #***** Did not find any application with associated appeals.
                def parse_appeals():
                    tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="Appeals"]')))
                    if scraper_name in ['MalvernHills', 'NorthDevon', 'Worcester', 'Wychavon']:
                        no_appeals = tab_panel.find_element(By.XPATH, '//*[@id="Appeals"]/div/table/tbody/tr/td').text.strip()
                    else: # 'Fylde'
                        no_appeals = tab_panel.find_element(By.XPATH, './h2').text.strip()

                    print(no_appeals) if PRINT else None

                parse_appeals()
            elif 'Map' in tab_name or 'Location' in tab_name:  # Leicester or WestmorlandFurness
                pass
            else:
                print(f'Unknown tab: {tab_name}')
                assert 0 == 1
        self.ending(app_df)



    """ Kent # https://www.kentplanningapplications.co.uk/Planning/Display/KCC/MA/0013/2011
    Feautres: [Main Details], [Map] 
        1. Encapsulated(1/1)
        Tab Main Page: Framework {item: dt, value: dd}, similar to Cumbria.
        
        2. No tab for other consultations, constraints, details.
        
        3. Encapsulated Doc system <get_documents_from_single_table>: in Main Details Page. (description_path is different from Crawley and Redcar)
        Single table with headers and document items mixed. Framework [date, description(with links)].
            #Type    Date
            #    Document items
            #Type    Date
            #    Document items
    """
    def parse_data_item_Kent(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        ### Ensure the page content is loaded.
        try:
            if 'Disclaimer' in response.xpath('//*[@id="main"]/div[1]/div/h3[2]/text()').get():
                # //*[@id="main"]/div[1]/div/h3[2]
                print('Click: Continue.')
                driver.find_element(By.XPATH, '//*[@id="main"]/div[1]/div/form/div/input').click()
                # //*[@id="main"]/div[1]/div/form/div/input
        except TypeError:
            pass
        print("parse_data_item_Kent")

        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)

        try:
            # --- --- --- Main Details --- --- ---
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="main"]/div[1]/div/div/div[2]/div[1]/dl')))
            items = tab_panel.find_elements(By.XPATH, './dt')
            item_values = tab_panel.find_elements(By.XPATH, './dd')
            n_items = len(items)
            print(f"\n1. Details Tab: {n_items}") if PRINT else None  # print(f"Details Tab: {n_items}")
            app_df = self.scrape_data_items(app_df, items, item_values)
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
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return
                # print('--- --- test --- ---')

        tabs = driver.find_elements(By.XPATH, '//*[@id="myTopnav"]/a')[:-1]
        app_df = self.set_default_items(app_df)

        [n_documents, file_urls, document_names, app_df] = self.get_documents_from_single_table(driver, app_df, folder_name, tab_index=-1, table_path='//*[@id="main"]/div[1]/div/div/div[2]/div[1]/table/tbody', description_path='./td[2]/a')
        if n_documents > 0:
            item = self.create_item(driver, folder_name, file_urls, document_names)
            yield item

        for tab_index, tab in enumerate(tabs[1:]):
            tab.click()
            tab_name = tab.text.strip()

            if 'Map' in tab_name:
                pass
            else:
                print('Unknown tab:', tab_name)
                assert 0 == 1

        self.ending(app_df)



    """ Lancashire # https://planningregister.lancashire.gov.uk/Planning/Display/06/09/0043  #***#  Applicants + Attachments
    Features:   [Main Details], [Applicants], [Consultees and Constraints], [Committee], [Attachments]
        
        1. Encapsulated(3/3)
        Tab Main Details: Framework {item: div/div/div[1]/label, value: div/div/div[2]/span}
        Tab Applicants: A neighbour list.
        
        Tab Committee: Has phone and email contacts.
        3. Encapsulated Doc system: Multi-tables with types as sub table names. [similar to Essex, Glamorgan] 
            #Shared Columns
            #Type1
            #    Document items. [date, description, file(with links)].
            #Type2
            #    Document items. [date, description, file(with links)].
    """
    def parse_data_item_Lancashire(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        ### Ensure the page content is loaded.
        """
        try:
            try:  # cookie
                driver.find_element(By.XPATH, '//*[@id="cookie-alert"]/span/button').click()
            except {NoSuchElementException, ElementNotInteractableException}:
                pass

            if 'Disclaimer' in response.xpath('//*[@id="topOfContent"]/h1/strong/text()').get():
                print('Click: Agree.')
                driver.find_element(By.XPATH, '//*[@id="topOfContent"]/form/div/input').click()
        except TypeError:
            pass
        """
        print("parse_data_item_Lancashire")

        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)

        try:
            # --- --- --- Main Details --- --- ---
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="detailspane"]/div')))
            items = tab_panel.find_elements(By.XPATH, './div/div/div[1]/label')
            item_values = tab_panel.find_elements(By.XPATH, './div/div/div[2]/span')
            n_items = len(items)
            print(f"\n1. Details Tab: {n_items}")  # if PRINT else None
            app_df = self.scrape_data_items(app_df, items, item_values)
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
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return
                # print('--- --- test --- ---')

        tabs = driver.find_elements(By.XPATH, '//*[@id="myTopnav"]/ul/li/a')
        app_df = self.set_default_items(app_df)

        for tab_index, tab in enumerate(tabs[1:]):
            tab.click()
            tab_name = tab.text.strip()
            # --- --- --- Applicants --- --- ---
            if 'Applicants' in tab_name:
                tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="applicant"]/div')))
                items = tab_panel.find_elements(By.XPATH, './div/div/div[1]/label')
                item_values = tab_panel.find_elements(By.XPATH, './div/div/div[2]/span')
                n_items = len(items)
                print(f"\n{tab_index+2}. Applicants Tab: {n_items}") # if PRINT else None
                app_df = self.scrape_data_items(app_df, items[:-1], item_values[:n_items-1])

                assert items[-1].text.strip() == 'Neighbour List'
                item_values = item_values[n_items-1:]
                if len(item_values) > 1 or item_values[0].text.strip() != 'No Neighbour':
                    content_dict = {'Neighbour List': [neighbour.text.strip() for neighbour in item_values]}
                    content_df = pd.DataFrame(content_dict)
                    content_df.to_csv(f"{self.data_storage_path}{folder_name}/neighbour list.csv", index=False)
            # --- --- --- Consultatees and constraints --- --- ---
            elif 'Consultee' in tab_name:
                def parse_consultations2():
                    consultation_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@id="consults"]/div/div')))
                    consultation_tables = consultation_table_list.find_elements(By.XPATH, './div')
                    n_consultation_tables = len(consultation_tables)
                    print(f"\n{tab_index+2}. Consultations Tab, {n_consultation_tables} tables.") if PRINT else None
                    dict_consultation_names = {  # 'Neighbour List':   'neighbour comments',
                                                'Consultee list': 'consultee comments',
                                                'Constraints list': 'constraints'}
                    n_table_items, csv_names = self.scrape_multi_tables_for_csv_inner_tablename(dict_consultation_names, consultation_tables, folder_name,
                                                                                                table_path='div', name_path='h3', column_path='table/thead/tr/th',
                                                                                                item_path='div', pre_item_path='div')


                def parse_consultations():
                    consultation_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@id="consults"]/div/div')))
                    consultation_tables = consultation_table_list.find_elements(By.XPATH, './div')
                    n_consultation_tables = len(consultation_tables)

                    consultation_list_names = [consultation_table.find_element(By.XPATH, './div[1]/h3').text.strip() for consultation_table in consultation_tables]
                    dict_consultation_names = {#'Neighbour List':   'neighbour comments',
                                               'Consultee list':    'consultee comments',
                                               'Constraints list':  'constraints'}
                    consultation_names = [dict_consultation_names[name] for name in consultation_list_names]
                    print(f"\n{tab_index+2}. Consultations Tab, {n_consultation_tables} tables: {consultation_names}") if PRINT else None

                    def scrape_consultations(table_name, table_columns, table_items):  # Similar to the one in parse_B but different in XPATH: f'./div[{column_index+1}]'
                        content_dict = {}
                        if len(table_columns) == 0:
                            column_names = ['Constraint content']
                        else:
                            column_names = [column.text.strip() for column in table_columns]
                        print(f'    {table_name}, {len(table_items)} items with column names: ', column_names) if PRINT else None
                        n_columns = len(column_names)

                        for column_index in range(n_columns):
                            content_dict[column_names[column_index]] = [item.find_element(By.XPATH, f'./td[{column_index+1}]').text.strip() for item in table_items]

                        content_df = pd.DataFrame(content_dict)
                        if 'neighbour' in table_name:
                            app_df.at['other_fields.n_comments_public_received'] = len(table_items)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/neighbour comments.csv", index=False)
                        elif 'consult' in table_name:
                            app_df.at['other_fields.n_comments_consultee_responded'] = len(table_items)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/consultee comments.csv", index=False)
                        elif 'constraint' in table_name:
                            app_df.at['other_fields.n_constraints'] = len(table_items)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/constraints.csv", index=False)
                        else:
                            assert 0 == 1  # throw out an exception for further investigation.
                    # scrape each table:
                    for table_index, consultation_table in enumerate(consultation_tables):
                        consultation_table_name = consultation_names[table_index]
                        consultation_table_columns = consultation_table.find_elements(By.XPATH, './div[2]/table/thead/tr/th')
                        consultation_table_items = consultation_table.find_elements(By.XPATH, './div[2]/table/tbody/tr')
                        if len(consultation_table_items) > 0:
                            scrape_consultations(consultation_table_name, consultation_table_columns, consultation_table_items)
                        else:
                            print(f"    {consultation_table_name} <NULL>: {consultation_table.find_element(By.XPATH, './div[2]/span').text.strip()}") if PRINT else None
                    app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at['other_fields.n_comments_consultee_responded']
                    #print(f"{tab_index+2}. Number of comments: {app_df.at['other_fields.n_comments']}. \n Number of constraints: { app_df.at['other_fields.n_constraints']}")
                parse_consultations()
            # --- --- --- Commitee --- --- ---
            elif 'Committee' in tab_name:  #
                tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="committee"]/div')))
                items = tab_panel.find_elements(By.XPATH, f'./div/div[1]/lable')  # Note it is lable, not label
                item_values = tab_panel.find_elements(By.XPATH, f'./div/div[2]/span')
                n_items = len(items)
                print(f"\n{tab_index+2}. Commitee Tab: {n_items}")  # if PRINT else None
                app_df = self.scrape_data_items_including_contacts_and_checkbox(app_df, items, item_values, folder_name,
                                                                                checkbox_items=['Application subject to Legal Agreement'])
            # --- --- --- Attachments --- --- ---
            elif 'Attachment' in tab_name:
                def get_documents():
                    try:
                        document_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@id="documents"]/div/div/table')))
                    except TimeoutException:  # There are no documents associated with this Planning application
                        app_df.at['other_fields.n_documents'] = 0
                        no_docs_string = driver.find_element(By.XPATH, '//*[@id="documents"]/div/div/p[2]').text.strip()
                        print(f"\n{tab_index+2}. <NULL> Attachment Tab: {no_docs_string}.") if PRINT else None
                        return 0, [], []

                    document_tables = document_table_list.find_elements(By.XPATH, './tbody')
                    n_tables = len(document_tables)
                    print(f"\n{tab_index+2}. Attachment Tab: {n_tables} tables") if PRINT else None

                    columns = document_table_list.find_elements(By.XPATH, './thead/tr/th')
                    [date_column, description_column] = self.get_column_indexes(columns, keywords=['date', 'description'])

                    n_documents, file_urls, document_names = 0, [], []
                    for table_index, document_table in enumerate(document_tables):
                        document_table_name = document_table.find_element(By.XPATH, './tr[1]/th').text.strip()
                        document_items = document_table.find_elements(By.XPATH, './tr')[1:]
                        n_table_documents = len(document_items)
                        print(f"Table {table_index+1}: {document_table_name}, including {n_table_documents} documents.") if PRINT else None
                        for document_item in document_items:
                            n_documents += 1
                            file_url = document_item.find_element(By.XPATH, f'./td[2]/a').get_attribute('href')
                            file_urls.append(file_url)

                            item_extension = file_url.split('.')[-1]
                            document_name = f"uid={n_documents}.{item_extension}"
                            document_name = self.rename_document_date_desc(document_item, document_name, document_type=document_table_name,
                                                                           description_column=description_column, date_column=date_column, path='td')
                            # document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
                            print(f"    Document {n_documents}: {document_name}") if PRINT else None
                            document_name = replace_invalid_characters(document_name)
                            document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                    app_df.at['other_fields.n_documents'] = n_documents
                    print(f'Total documents: {n_documents}')  # if PRINT else None
                    return n_documents, file_urls, document_names
                n_documents, file_urls, document_names = get_documents()
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            else:
                print('Unknown tab:', tab_name)
                assert 0 == 1
        self.ending(app_df)



    """ Leicester, https://planning.leicester.gov.uk/Planning/Display/20080005
    Similar to Fylde, Differences: 1) XPATHs; 2) Phone contact; 3) Document groups in a tbody; 4) No consultation; 5) Has a Map tab.
        Planning Online Status | [Application Details(Summary/Important Dates/Further Information/ Condition Details//Information Notes)], 
                                 [Documents], ([Consultations]), ([Map]), [Appeals] 
        1. Encapsulated(0/1): both item and item_value use ./td
    
    
        3. Doc system: Single table with headers and document items mixed.
        header class_name is different, so have not encapsulated to <get_documents_from_single_table>.
    """
    def parse_data_item_Leicester(self, response):
        ### Ensure the page content is loaded.
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]

        print(f"parse_data_item_Leicester")
        try:
            if 'Disclaimer' in response.xpath('//*[@id="skipToContent"]/h1/text()').get():
                print('Click: Agree the Disclaimer.')
                driver.find_element(By.XPATH, '//*[@id="skipToContent"]/div[1]/form/div/input').click()
        except TypeError:
            pass
        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)

        try:
            # --- --- --- Application Details (Summary, Important Dates, Further Information, Conditions) --- --- ---
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="detailspane"]')))
            tables = tab_panel.find_elements(By.XPATH, './div/table')
            n_tables = len(tables)
            print(f"\n1. Application Details Tab: {n_tables} tables.")  # if PRINT else None #print(f"Main Details Tab: {n_items}")
            for table_index, table in enumerate(tables):
                items = table.find_elements(By.XPATH, './tbody/tr')
                table_name = items[0].find_element(By.XPATH, './td').text.strip()
                print(f"  Table {table_index+1}: {table_name}") if PRINT else None
                contact_dict = {}
                for item_index, item in enumerate(items[1:]):
                    item_name = item.find_element(By.XPATH, './td[1]').text.strip()
                    data_name = self.details_dict[item_name]
                    item_value = item.find_element(By.XPATH, './td[2]').text.strip()
                    # print(f"    {item_index}: {item_name}, {item_value}, {type(item_name)}")
                    # If data_name in self.app_dfs.columns:
                    try:
                        if data_name == 'Phone':
                            contact_dict[data_name] = [item_value]
                            contact_df = pd.DataFrame(contact_dict)
                            contact_df.to_csv(f"{self.data_storage_path}{folder_name}/contact.csv", index=False)
                            print(f"    <{item_name}> scraped (csv): {item_value}") if PRINT else None
                        else:
                            app_df.at[data_name] = item_value
                            print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                    # New
                    except KeyError:
                        app_df[data_name] = item_value
                        print(f"    <{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
            assert app_df.at['other_fields.appeal_result'] == 'N/A'
        except TimeoutException:  # ***** situation to be completed: application not available
            # Planning Application details not available . e.g. auth=123, year=[21:]
            note = response.xpath('//*[@id="pageheading"]/h1/text()').get()
            print('note: ', note)
            # This application is no longer available for viewing. It may have been removed or restricted from public viewing.
            if note is not None and 'details not available' in note:
                print('*** *** *** This application is not available. *** *** ***')
                return
            else:
                print('*** *** *** NEED TO RELOAD APP PAGE. *** *** ***')
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return

        tabs = driver.find_elements(By.XPATH, '//*[@id="displaypage"]/div[4]/ul/li')
        app_df = self.set_default_items(app_df)
        for tab_index, tab in enumerate(tabs[1:]):
            tab.find_element(By.XPATH, './a').click()
            tab_name = tab.find_element(By.XPATH, './a').text.strip()
            # print(f"tab {tab_index + 1}: {tab_name}")
            # --- --- --- Documents --- --- ---
            if 'Documents' in tab_name:  # Adaptive columns, link with doc type, multiple tables in a tbody.
                def get_supporting_documents():
                    try:
                        document_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="documentspane"]/div/table')))
                    except TimeoutException:  # ***** situation to be completed: No documents
                        app_df.at['other_fields.n_documents'] = 0
                        print(f"\n{tab_index+2}. <NULL> Document Tab: {app_df.at['other_fields.n_documents']} items.") if PRINT else None
                        return 0, [], []

                    document_tables = document_table_list.find_elements(By.XPATH, './tbody')
                    n_tables = len(document_tables)
                    print(f"\n{tab_index+2}. Document Tab: {n_tables} tables") if PRINT else None

                    columns = document_table_list.find_elements(By.XPATH, './thead/tr/th')
                    [date_column, type_column, description_column] = self.get_column_indexes(columns, keywords=['date', 'type', 'description'])

                    n_documents, file_urls, document_names = 0, [], []
                    #driver.find_element(By.CLASS_NAME, 'copyright-agreement').click()
                    for table_index, document_table in enumerate(document_tables):
                        #document_table_name = document_table.find_element(By.XPATH, './tr[1]/th[2]/strong').text.strip()
                        document_table_names = document_table.find_elements(By.XPATH, './tr/th[2]/strong')
                        document_items = document_table.find_elements(By.XPATH, './tr')#[1:]
                        #n_table_documents = len(document_items)
                        n_table_documents = len(document_items) - len(document_table_names)
                        print(f"Table {table_index+1} includes {n_table_documents} documents.") if PRINT else None
                        table_name_counter = 0
                        for document_item in document_items:
                            try:
                                document_table_name = document_item.find_element(By.XPATH, './th[2]/strong').text.strip()
                                print(f"    Group name: {document_table_name}")
                                table_name_counter += 1
                            except NoSuchElementException:
                                n_documents += 1
                                file_url = document_item.find_element(By.XPATH, f'./td[{type_column}]/span/a').get_attribute('href')
                                file_urls.append(file_url)

                                item_identity = file_url.split('=')[-1]  # includes extension such as .pdf
                                document_name = f"uid={item_identity}"
                                try:
                                    document_description = document_item.find_element(By.XPATH, f'./td[{description_column}]').text.strip()
                                    document_name = f"desc={document_description}&{document_name}"
                                except NoSuchElementException:
                                    pass
                                document_type = document_item.find_element(By.XPATH, f'./td[{type_column}]').text.strip()
                                document_name = f"type={document_type}&{document_name}"
                                try:
                                    document_date = document_item.find_element(By.XPATH, f'./td[{date_column}]').text.strip()
                                    document_name = f"date={document_date}&{document_name}"
                                except NoSuchElementException:
                                    pass
                                # document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
                                print(f"    Document {n_documents}: {document_name}") if PRINT else None
                                document_name = replace_invalid_characters(document_name)
                                document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                    app_df.at['other_fields.n_documents'] = n_documents
                    print(f'Total documents: {n_documents}') if PRINT else None
                    return n_documents, file_urls, document_names
                n_documents, file_urls, document_names = get_supporting_documents()
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            # --- --- --- Appeals --- --- --- for a given documents framework, its different document tables share the same columns.
            elif 'Appeals' in tab_name:  # ***** Did not find any application with associated appeals.
                def parse_appeals():
                    tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="appealspane"]')))
                    no_appeals = tab_panel.find_element(By.XPATH, './h2').text.strip()
                    print(no_appeals) if PRINT else None
                parse_appeals()
            elif 'Map' in tab_name:  # Leicester
                pass
            else:
                print('Unknown tab:', tab_name)
                assert 0 == 1
        self.ending(app_df)



    """ Leicestershire # https://leicestershire.planning-register.co.uk/Planning/Display?applicationNumber=2014%2F0001%2F06 
    Features:  Mage Page | Multiple Document Tabs.  Similar to Cumbria.
        1. Encapsulated(1/1)
        Tab Main Page: Framework {item: tr/td[1], value: tr/td[2]}
        
        2. (0/0): No tab for other consultations, constraints, details.
        
        3. Encapsulated Doc system: Multiple tabs for documents. 
        item_extension need an extra operation.
    """
    def parse_data_item_Leicestershire(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        ### Ensure the page content is loaded.
        try:
            if 'Disclaimer' in response.xpath('//*[@id="node-218706"]/div/div/div/div/div/div/h4/text()').get():
                print('Click: Agree.')
                driver.find_element(By.XPATH, '//*[@id="node-218706"]/div/div/div/div/div/div/div/form/div/input').click()
        except TypeError:
            pass
        print("parse_data_item_Leicestershire")

        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)

        try:
            panels = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="node-218706"]/div/div/div/div/div/div/fieldset')))
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
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return
                # print('--- --- test --- ---')

        # --- --- --- Main Details --- --- ---
        tab_panel = panels.find_element(By.XPATH, './table/tbody')
        items = tab_panel.find_elements(By.XPATH, './tr/td[1]')
        item_values = tab_panel.find_elements(By.XPATH, './tr/td[2]')
        n_items = len(items)
        print(f"\n1. Details Tab: {n_items}") # if PRINT else None
        app_df = self.scrape_data_items_including_contacts(app_df, items, item_values, folder_name)

        app_df = self.set_default_items(app_df)
        # --- --- --- Other Tabs --- --- ---
        try:
            doc_tabs = driver.find_element(By.CLASS_NAME, 'topnav').find_elements(By.XPATH, './a')[:-1]
            n_documents, file_urls, document_names = 0, [], []
            for tab_index, tab in enumerate(doc_tabs):
                tab.click()
                tab_name = tab.text.strip()
                def get_supporting_documents_from_tab(tab_index, tab_name, n_documents, file_urls, document_names):
                    document_table = panels.find_element(By.XPATH, f'./div[2]/div[{tab_index+1}]/table/tbody')
                    document_items = document_table.find_elements(By.XPATH, './tr')
                    n_table_documents = len(document_items)
                    print(f"\nDocument Tab {tab_index+1}: <{tab_name}>, including {n_table_documents} documents.") if PRINT else None
                    for document_item in document_items:
                        n_documents += 1
                        file_url = document_item.find_element(By.XPATH, f'./td[2]/a').get_attribute('href')
                        file_urls.append(file_url)

                        item_extension = file_url.split('.')[-1]
                        item_extension = item_extension.split('&')[0]
                        document_name = f"uid={n_documents}.{item_extension}"
                        document_name = self.rename_document_date_desc(document_item, document_name, document_type= tab_name,
                                                                       description_column=2, date_column=3, path='td')
                        # document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
                        print(f"    Document {n_documents}: {document_name}") if PRINT else None
                        document_name = replace_invalid_characters(document_name)
                        document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                    return n_documents, file_urls, document_names
                n_documents, file_urls, document_names = get_supporting_documents_from_tab(tab_index, tab_name, n_documents, file_urls, document_names)
            app_df.at['other_fields.n_documents'] = n_documents
            print(f'Total documents: {n_documents}') if PRINT else None
            if n_documents > 0:
                item = self.create_item(driver, folder_name, file_urls, document_names)
                yield item
        except NoSuchElementException:
            app_df.at['other_fields.n_documents'] = 0
            print(f"No Document Tab.") if PRINT else None
        self.ending(app_df)


    """ Lincolnshire # https://lincolnshire.planning-register.co.uk/Disclaimer?returnUrl=%2FPlanning%2FDisplay%3FapplicationNumber%3DPL%255C0091%255C06 
    Features: [Main Details], [Associated Documents], [Consultees] 
        1. Encapsulated(1/1)
        Tab Main Page: Framework {item: dt, value: dd}
       
        2. Encapsulated(1/1): [Consultees]
       
        3. Encapsulated Doc system: Multi-tables with types as sub table names.
            #Shared Columns
            #Type1
            #    Document items [file(with links)].
            #Type2
            #    Document items [file(with links)]. 
    """
    def parse_data_item_Lincolnshire(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        ### Ensure the page content is loaded.
        try:
            if 'Disclaimer' in response.xpath('/html/body/div/div/h1/text()').get():
                print('Click: Agree.')
                driver.find_element(By.XPATH, '/html/body/div/div/form/div/input').click()
        except TypeError:
            pass
        print("parse_data_item_Lincolnshire")

        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)

        try:
            # --- --- --- Main Details --- --- ---
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="tab-content"]/div[1]/div/dl')))
            items = tab_panel.find_elements(By.XPATH, './dt')
            item_values = tab_panel.find_elements(By.XPATH, './dd')
            n_items = len(items)
            print(f"\n1. Main Details Tab: {n_items}")  # if PRINT else None
            app_df = self.scrape_data_items(app_df, items, item_values)
        except TimeoutException:  # ***** situation to be completed: application not available
            # Planning Application details not available . e.g. auth=123, year=[21:]
            note = response.xpath('//*[@id="pageheading"]/h1/text()').get()
            print('note: ', note)
            # This application is no longer available for viewing. It may have been removed or restricted from public viewing.
            if note is not None and 'details not available' in note:
                print('*** *** *** This application is not available. *** *** ***')
                return
            else:
                print('*** *** *** NEED TO RELOAD APP PAGE. *** *** ***')
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return

        tabs = driver.find_elements(By.XPATH, '//*[@id="myTopnav"]/a')[:-1]
        app_df = self.set_default_items(app_df)
        for tab_index, tab in enumerate(tabs[1:]):
            tab.click()
            tab_name = tab.text.strip()
            # print(f"tab {tab_index + 1}: {tab_name}")
            # --- --- --- Associate Documents --- --- ---
            if 'Documents' in tab_name:  # Adaptive columns, link with doc type, multiple tables in a tbody.
                def get_supporting_documents():
                    try:
                        document_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@id="tab-content"]/div[{tab_index+2}]/div/table')))
                    except TimeoutException:
                        app_df.at['other_fields.n_documents'] = 0
                        print(f"\n{tab_index+2}. <NULL>  Document Tab: {app_df.at['other_fields.n_documents']} items.") if PRINT else None
                        return 0, [], []

                    document_tables = document_table_list.find_elements(By.XPATH, './tbody')
                    n_tables = len(document_tables)
                    print(f"\n{tab_index+2}. Document Tab: {n_tables} tables") if PRINT else None

                    columns = document_table_list.find_elements(By.XPATH, './thead/tr/th')
                    [date_column, description_column] = self.get_column_indexes(columns, keywords=['date', 'file'])

                    n_documents, file_urls, document_names = 0, [], []
                    for table_index, document_table in enumerate(document_tables):
                        document_table_name = document_table.find_element(By.XPATH, './tr[1]/th/span[2]').text.strip()
                        document_items = document_table.find_elements(By.XPATH, './tr')[1:]
                        n_table_documents = len(document_items)
                        print(f"Table {table_index+1}: {document_table_name}, including {n_table_documents} documents.") if PRINT else None
                        for document_item in document_items:
                            n_documents += 1
                            file_url = document_item.find_element(By.XPATH, f'./td[{description_column}]/a').get_attribute('href')
                            file_urls.append(file_url)

                            item_extension = file_url.split('.')[-1]
                            document_name = f"uid={n_documents}.{item_extension}"
                            document_name = self.rename_document_date_desc(document_item, document_name, document_type=document_table_name,
                                                                           description_column=description_column, date_column=date_column, path='td')
                            print(f"    Document {n_documents}: {document_name}") if PRINT else None
                            document_name = replace_invalid_characters(document_name)
                            document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                    app_df.at['other_fields.n_documents'] = n_documents
                    print(f'Total documents: {n_documents}') if PRINT else None
                    return n_documents, file_urls, document_names
                n_documents, file_urls, document_names = get_supporting_documents()
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            elif 'Consultee' in tab_name:
                def parse_consultees():
                    try:
                        consultees_table = driver.find_element(By.XPATH, f'//*[@id="tab-content"]/div[{tab_index+2}]/table')
                        table_items = consultees_table.find_elements(By.XPATH, './tbody/tr')
                        n_items = len(table_items)
                        print(f"\n{tab_index+2}. Consultees Tab: {n_items}") if PRINT else None
                        if n_items > 0:
                            app_df.at['other_fields.n_comments_consultee_responded'] = n_items
                            app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at[
                                'other_fields.n_comments_consultee_responded']
                            table_columns = consultees_table.find_elements(By.XPATH, './thead/tr/th')
                            self.scrape_for_csv(csv_name='consultee comments', table_columns=table_columns, table_items=table_items,
                                                folder_name=folder_name, path='td')
                    except NoSuchElementException:  # No Consultations found for this Application
                        print(f"\n{tab_index+2}. " + driver.find_element(By.XPATH, f'//*[@id="tab-content"]/div[{tab_index+2}]/p').text.strip())
                parse_consultees()
            else:
                print('Unknown tab: ', tab_name)
                assert 0 == 1
        self.ending(app_df)



    """ Somerset # https://planning.somerset.gov.uk/Planning/Display?applicationNumber=4%2F38%2F04%2F128#undefined
    Feautres: [Main Details], [Consultations], [Map], [Associate Documents]
        1. Encapsulated(1/1)
        Tab Main Page: Framework {item: label, value: div} 
        
        Tab Consultations: multiple tables for neighbour, consltee comments.
        3. Encapsulated Doc system: Multi-tables with types as sub table names. [Similar to Essex, Glamorgan, Lancashire except column names] 
            #Shared Columns
            #Type1
            #    Document items [date, description(with links)].
            #Type2
            #    Document items [date, description(with links)].
     """
    def parse_data_item_Somerset(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        ### Ensure the page content is loaded.
        try:
            try:  # cookie
                driver.find_element(By.XPATH, '//*[@id="cookie-alert"]/span/button').click()
            except {NoSuchElementException, ElementNotInteractableException}:
                pass

            if 'Disclaimer' in response.xpath('//*[@id="topOfContent"]/h1/strong/text()').get():
                print('Click: Agree.')
                driver.find_element(By.XPATH, '//*[@id="topOfContent"]/form/div/input').click()
        except TypeError:
            pass
        print("parse_data_item_Somerset")

        # self.get_doc_url(response, app_df)
        # --- --- --- setup the app storage path. --- --- ---
        folder_name = self.setup_storage_path(app_df)

        try:
            # --- --- --- Main Details --- --- ---
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="topOfContent"]/div[4]/div[1]')))
            items = tab_panel.find_elements(By.XPATH, './label')
            item_values = tab_panel.find_elements(By.XPATH, './div')
            n_items = len(items)
            print(f"\n1. Details Tab: {n_items}") # if PRINT else None
            app_df = self.scrape_data_items(app_df, items, item_values)
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
                # self.index -= 1
                time.sleep(10)
                # yield SeleniumRequest(url=app_df.at['url'], callback=self.re_parse_summary_item, meta={'app_df': app_df})
                return
                # print('--- --- test --- ---')

        tabs = driver.find_elements(By.XPATH, '//*[@id="myTopnav"]/a')[:-1]
        app_df = self.set_default_items(app_df)

        for tab_index, tab in enumerate(tabs[1:]):
            tab.click()
            tab_name = tab.text.strip()
            if 'Consultation' in tab_name:
                def parse_consultations():
                    consultation_table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@id="topOfContent"]/div[4]/div[{tab_index+2}]')))

                    consultation_list_names = consultation_table_list.find_elements(By.XPATH, './h3')
                    consultation_list_names = [name.text.strip() for name in consultation_list_names]
                    dict_consultation_names = {'Neighbour List': 'neighbour comments',
                                               'Consultee List': 'consultee comments'}
                    consultation_names = [dict_consultation_names[name] for name in consultation_list_names]

                    consultation_tables = consultation_table_list.find_elements(By.XPATH, './table')
                    n_consultation_tables = len(consultation_tables)
                    print(f"\n{tab_index+2}. Consultations Tab, {n_consultation_tables} tables: {consultation_names}") if PRINT else None

                    def scrape_consultations(table_name, table_columns, table_items):  # Similar to the one in parse_B but different in XPATH: f'./div[{column_index+1}]'
                        content_dict = {}
                        column_names = [column.text.strip() for column in table_columns]
                        print(f'{table_name}, {len(table_items)} items with column names: ', column_names) if PRINT else None
                        n_columns = len(column_names)

                        for column_index in range(n_columns):
                            content_dict[column_names[column_index]] = [item.find_element(By.XPATH, f'./tr/td[{column_index+1}]').text.strip() for item in table_items]

                        content_df = pd.DataFrame(content_dict)
                        if 'neighbour' in table_name:
                            app_df.at['other_fields.n_comments_public_received'] = len(table_items)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/neighbour comments.csv", index=False)
                        elif 'consult' in table_name:
                            app_df.at['other_fields.n_comments_consultee_responded'] = len(table_items)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/consultee comments.csv", index=False)
                        elif 'constraint' in table_name:
                            app_df.at['other_fields.n_constraints'] = len(table_items)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/constraints.csv", index=False)
                        else:
                            assert 0 == 1  # throw out an exception for further investigation.

                    # scrape each table:
                    for table_index, consultation_table in enumerate(consultation_tables):
                        consultation_table_name = consultation_names[table_index]
                        consultation_table_columns = consultation_table.find_elements(By.XPATH, './thead/tr/th')
                        consultation_table_items = consultation_table.find_elements(By.XPATH, './tbody')

                        if len(consultation_table_items) > 0 and len(consultation_table_items[0].find_elements(By.XPATH, './tr/td'))>1:
                            scrape_consultations(consultation_table_name, consultation_table_columns, consultation_table_items)
                        else:  # ***** To be validate
                            print(f"{consultation_table_name} <NULL>") if PRINT else None
                    app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at['other_fields.n_comments_consultee_responded']
                    print(f"{tab_index+2}. Number of comments: {app_df.at['other_fields.n_comments']}")
                parse_consultations()
            elif 'Map' in tab_name:
                pass
            elif 'Document' in tab_name:
                def get_documents():
                    try:
                        document_table_list = driver.find_element(By.XPATH, f'//*[@id="documents"]/div/div/div/table')
                    except NoSuchElementException:  # There are currently no scanned documents for this application.
                        app_df.at['other_fields.n_documents'] = 0
                        no_docs_string = driver.find_element(By.XPATH, '//*[@id="documents"]/div/div/div/div[2]/label').text.strip()
                        print(f"\n{tab_index+2}. <NULL> Document Tab: {no_docs_string}.") if PRINT else None
                        return 0, [], []

                    document_tables = document_table_list.find_elements(By.XPATH, './tbody')
                    n_tables = len(document_tables)
                    print(f"\n{tab_index+2}. Document Tab: {n_tables} tables") if PRINT else None

                    columns = document_table_list.find_elements(By.XPATH, './thead/tr/th')
                    [date_column, description_column] = self.get_column_indexes(columns, keywords=['date', 'file name'])

                    n_documents, file_urls, document_names = 0, [], []
                    for table_index, document_table in enumerate(document_tables):
                        document_table_name = document_table.find_element(By.XPATH, './tr[1]/th').text.strip()
                        document_items = document_table.find_elements(By.XPATH, './tr')[1:]
                        n_table_documents = len(document_items)
                        print(f"Table {table_index+1}: {document_table_name}, including {n_table_documents} documents.") if PRINT else None
                        for document_item in document_items:
                            n_documents += 1
                            file_url = document_item.find_element(By.XPATH, f'./td[{description_column}]/a').get_attribute('href')
                            file_urls.append(file_url)

                            item_extension = file_url.split('.')[-1]
                            document_name = f"uid={n_documents}.{item_extension}"
                            document_name = self.rename_document_date_desc(document_item, document_name, document_type=document_table_name,
                                                                           description_column=description_column, date_column=date_column, path='td')
                            # document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
                            print(f"    Document {n_documents}: {document_name}") if PRINT else None
                            document_name = replace_invalid_characters(document_name)
                            document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                    app_df.at['other_fields.n_documents'] = n_documents
                    print(f'\n{tab_index+2}. Total documents: {n_documents}') # if PRINT else None
                    return n_documents, file_urls, document_names
                n_documents, file_urls, document_names = get_documents()
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            else:
                print('Unknown tab:', tab_name)
                assert 0 == 1

        self.ending(app_df)