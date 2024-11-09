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
from pathlib import Path



class PlanningExplorer_Scraper(scrapy.Spider):
    name = 'PlanningExplorer_Scraper'

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

        if DEVELOPMENT_MODE:
            auth_names = get_scraper_by_type('PlanningExplorer')  # PlanningExplorer  # Custom 51
            auth_names = [auth_name for auth_name in auth_names if not auth_name.startswith('.')]
            auth_names.sort(key=str.lower)
            print(auth_names)

            app_dfs = []
            self.auth_index = 1  #1 Birmingham, 3 Camden, 4 Charnwood, 7 Islington, 8 Merton, 13 Wandsworth

            self.year = -1
            self.auth = auth_names[int(self.auth_index)]
            self.data_storage_path, self.data_upload_path = initialize_paths(self.data_storage_path, self.auth, self.year)

            sample_index = 4
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

        #""" for IP rotation
        valid_IP_proxy_path = f'{Path(get_project_root()).parent}/valid_IPs/{self.auth}.csv'
        if os.path.exists(valid_IP_proxy_path):
            valid_IPs_df = pd.read_csv(valid_IP_proxy_path)
            self.init_valid_IPs = valid_IPs_df.iloc[:, 0].tolist()
            self.n_init_valid_IPs = len(self.init_valid_IPs)
        else:
            self.init_valid_IPs = None
            self.n_init_valid_IPs = 0
        # """ # end of IP rotation

        self.parse_func = self.parse_data_item_Birmingham
        if auth_names[self.auth_index] in ['Wandsworth']:
            self.parse_func = self.parse_data_item_Wandsworth
        elif auth_names[self.auth_index] in ['Camden']:
            self.parse_func = self.parse_data_item_Camden
        elif auth_names[self.auth_index] in ['Merton']:
            self.parse_func = self.parse_data_item_Merton
        elif auth_names[self.auth_index] in ['Islington']:
            self.parse_func = self.parse_data_item_Islington


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(PlanningExplorer_Scraper, cls).from_crawler(crawler, *args, **kwargs)
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
            # yield SeleniumRequest(url=url, callback=self.parse_data_item, meta={'app_df': app_df})
            #yield SeleniumRequest(url=url, callback=self.parse_func, meta={'app_df': app_df})
            yield SeleniumRequest(url=url, callback=self.parse_func, meta={'app_df': app_df, 'valid_IPs': self.init_valid_IPs})
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

    details_dict = {'Site Address': 'address',
                    'Application Registered': 'other_fields.date_registered',
                    "Council's Decision (decision)": 'other_fields.decision', # Birmingham
                    "Council's Decision (date)": 'other_fields.decision_issued_date',
                    'Decision (decision)': 'other_fields.decision', # Wandsworth
                    'Decision (date)': 'other_fields.decision_issued_date',
                    'Application Number': 'uid',
                    'Application Type': 'other_fields.application_type',
                    'Proposal': 'description',
                    'Area Team': 'other_fields.area_team', #
                    'Ward': 'other_fields.ward_name',
                    'Wards': 'other_fields.ward_name',
                    'Constituency': 'other_fields.constituency', #

                    'Comments Until': 'other_fields.comment_expires_date',
                    'Date of Committee': 'other_fields.meeting_date',
                    'Development Type': 'other_fields.development_type',
                    'Current Status': 'other_fields.status',
                    'Parishes': 'other_fields.parish',
                    'Division': 'other_fields.division', #
                    'Determination Level': 'other_fields.determination_level', #
                    'Existing Land Use': 'other_fields.existing_land_use', #
                    'Proposed Land Use': 'other_fields.proposed_land_use', #
                    # OS Mapsheet, Recommendation
                    'OS Mapsheet': 'other_field.os_mapsheet', #
                    'Recommendation': 'other_field.recommendation', #

                    # Applicant & Agent
                    'Applicant': 'other_fields.applicant_name',
                    'Planning Officer': 'other_fields.case_officer',
                    'Agent': 'other_fields.agent_name',

                    # Location
                    'Location Co ordinates (easting)': 'other_fields.easting',
                    'Location Co ordinates (northing)': 'other_fields.northing',

                    # Appeal
                    'Appeal Submitted?': 'other_fields.appeal_status', #
                    'Appeal Decision': 'other_fields.appeal_result',
                    'Appeal Date Lodged': 'other_fields.appeal_date_lodged', # Birmingham
                    'Appeal Lodged': 'other_fields.appeal_lodged', # Wandsworth
                    'Appeal Decision Date': 'other_fields.appeal_decision_date',

                    # Dates
                    'Received': 'other_fields.date_received',
                    'Registered': 'other_fields.date_registered', #
                    'Valid From': 'other_fields.date_valid',
                    'Statutory Expiry Date':  'other_fields.statutory_expires_date', #
                    'Decision Expiry': 'other_fields.decision_due_date',

                    'First Advertised': 'other_fields.first_advertised',
                    'First Site Notice': 'other_fields.site_notice_start_date',
                    'Date of First Consultation': 'other_fields.consultation_start_date',
                    'Consultation Expiry': 'other_fields.consultation_end_date',
                    'Expiry Date': 'other_fields.application_expires_date',
                    'Valid': 'other_fields.date_valid',
                    'Validated': 'other_fields.date_validated',

                    # Meeting
                    'First Council': 'other_fields.first_council',
                    'First Committee': 'other_fields.first_committee',
                    'Last Council': 'other_fields.last_council',
                    'Last Committee':'other_fields.last_committee',

                    # Contacts
                    'Case Officer / Tel': 'Tel'
                    }

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
        app_df['other_fields.n_comments_public_received'] = 'Not available'
        app_df['other_fields.n_comments_public_objections'] = 'Not available'
        app_df['other_fields.n_comments_public_supporting'] = 'Not available'
        app_df['other_fields.n_comments_consultee_total_consulted'] = 'Not available' # 0
        app_df['other_fields.n_comments_consultee_responded'] = 'Not available'
        app_df.at['other_fields.n_comments'] = 0
        app_df['other_fields.n_public_notices'] = 'Not available'

        app_df.at['other_fields.n_constraints'] = 'Not available'
        app_df.at['other_fields.n_documents'] = 0
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


    def parse_date_item(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        driver = response.request.meta["driver"]
        tab_names = response.meta['tab_names']
        tab_urls = response.meta['tab_urls']

        data_blocks = driver.find_elements(By.CLASS_NAME, 'dataview')
        items = []
        for data_block in data_blocks[1:-1]:
            items.extend(data_block.find_elements(By.XPATH, './ul/li/div'))

        n_items = len(items)
        print(f"\n{tab_names[0]} Tab: {n_items} from {len(data_blocks)-2} blocks.")
        for i, item in enumerate(items):
            item_name = item.find_element(By.XPATH, './span').text.strip()
            data_name = self.details_dict[item_name]
            item_value = driver.execute_script("return arguments[0].lastChild.textContent;", item).strip()
            # if data_name in self.app_dfs.columns:
            try:
                app_df.at[data_name] = item_value
                print(f"<{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
            # New or contact
            except KeyError:
                app_df[data_name] = item_value
                print(f"<{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
        return app_df, folder_name, tab_names, tab_urls

    # Birmingham
    def get_next_parse_func_Birmingham(self, tab_name):
        if tab_name == 'Application Dates':
            next_parse_func = self.parse_date_item_Birmingham
        #elif tab_name == 'Application Meetings':
        #    next_parse_func = self.parse_date_item_Birmingham
        #elif tab_name == 'Application Constraints':
        #    next_parse_func = self.parse_constraint_Birmingham
        #elif tab_name == 'Application Site History':
        #    next_parse_func = self.parse_site_history_Birmingham
        #elif tab_name == 'Application Refusal Reasons':
        #    next_parse_func = self.parse_refusal_reasons_Birmingham
        elif tab_name == 'Consultees Details':
            next_parse_func = self.parse_consultee_item_Birmingham
        #elif tab_name == 'Neighbours Details':
        #    next_parse_func = self.parse_neighbour_item_Birmingham
        elif tab_name == 'View Associated Documents':
            next_parse_func = self.parse_document_item_Birmingham
            time.sleep(30)
        else:
            print(f'Unknown tab name: {tab_name}')
            assert 1 == 0
        return next_parse_func

    def parse_data_item_Birmingham(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        print(f"parse_data_item_Birmingham, scraper name: {scraper_name}")

        folder_name = self.setup_storage_path(app_df)
        # //*[@id="Template"]/div/div/div[2]/div/div/div[3]

        try:
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="Template"]/div/div/div[2]/div/div/div[3]')))
            data_blocks = tab_panel.find_elements(By.XPATH, './div')
        except TimeoutException:
            print('*** *** *** This application is not available. *** *** ***')
            return

        items = []  # data_blocks[0].find_elements(By.XPATH, './div')
        for data_block in data_blocks[1:-1]:
            items.extend(data_block.find_elements(By.XPATH, './ul/li/div'))

        n_items = len(items)
        print(f"\n1. Details Tab: {n_items} from {len(data_blocks)-2} blocks.")  # if PRINT else None #print(f"Main Details Tab: {n_items}")
        for i, item in enumerate(items):
            item_name = item.find_element(By.XPATH, './span').text.strip()
            if item_name == "Council's Decision":
                data_name = self.details_dict[item_name + ' (decision)']
                item_values = driver.execute_script("return arguments[0].lastChild.textContent;", item).strip().split('\n')

                app_df.at[data_name] = item_values[0].strip()
                print(f"<{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None

                data_name = self.details_dict[item_name + ' (date)']
                try:
                    item_value = item_values[1].strip()
                except IndexError:
                    item_value = ''
            else:
                data_name = self.details_dict[item_name]
                item_value = driver.execute_script("return arguments[0].lastChild.textContent;", item).strip()
            # if data_name in self.app_dfs.columns:
            try:
                app_df.at[data_name] = item_value
                print(f"<{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
            # New or contact
            except KeyError:
                app_df[data_name] = item_value
                print(f"<{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None

        app_df = self.set_default_items(app_df)
        tabs = data_blocks[-1].find_elements(By.XPATH, './ul/li/div/a')
        tab_names = [tab.text.strip() for tab in tabs]
        tab_urls = [tab.get_attribute('href') for tab in tabs]
        for i in range(len(tabs)):
            print(f'tab {i}: {tab_names[i]}')

        next_parse_func = self.get_next_parse_func_Birmingham(tab_names[0])
        yield SeleniumRequest(url=tab_urls[0], callback=next_parse_func,
                              meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names, 'tab_urls': tab_urls})

    def parse_date_item_Birmingham(self, response):
        app_df, folder_name, tab_names, tab_urls = self.parse_date_item(response)

        next_parse_func = self.get_next_parse_func_Birmingham(tab_names[1])
        yield SeleniumRequest(url=tab_urls[1], callback=next_parse_func,
                              meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names[1:], 'tab_urls': tab_urls[1:]})
    # specific
    def parse_consultee_item_Birmingham(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        driver = response.request.meta["driver"]
        tab_names = response.meta['tab_names']
        tab_urls = response.meta['tab_urls']

        tab_panel = driver.find_element(By.XPATH, '//*[@id="Template"]/div/div/div[2]/div/div/div[3]')
        data_blocks = tab_panel.find_elements(By.XPATH, './div')
        assert len(data_blocks) == 3
        items = data_blocks[1].find_elements(By.XPATH, './ul')  #/li/div')
        n_items = len(items)

        app_df['other_fields.n_comments_consultee_total_consulted'] = n_items
        app_df.at['other_fields.n_comments'] = n_items
        print(f"\n3. Consultee Tab: {n_items} comments.")
        if n_items > 0:
            content_dict = {}
            column_names = [column_li.find_element(By.XPATH, './div/span').text.strip() for column_li in items[0].find_elements(By.XPATH, './li')]
            n_columns = len(column_names)

            for column_index in range(n_columns):
                content_dict[column_names[column_index]] = [driver.execute_script("return arguments[0].lastChild.textContent;", item.find_element(By.XPATH, f'./li[{column_index+1}]/div')).strip()
                                                            for item in items]

            content_df = pd.DataFrame(content_dict)
            content_df.to_csv(f"{self.data_storage_path}{folder_name}/{data_blocks[1].find_element(By.XPATH, './h1').text.strip()}.csv", index=False)

        next_parse_func = self.get_next_parse_func_Birmingham(tab_names[1])
        yield SeleniumRequest(url=tab_urls[1], callback=next_parse_func,
                              meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names[1:], 'tab_urls': tab_urls[1:]})

    def parse_document_item_Birmingham(self, response):  # Need IP proxies.
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        driver = response.request.meta["driver"]
        #tab_urls = response.meta['tab_urls']

        def create_item(driver, folder_name, file_urls, document_names, IP_index=0):
            if not os.path.exists(self.failed_downloads_path + folder_name):
                os.mkdir(self.failed_downloads_path + folder_name)

            item = DownloadFilesItem()
            item['file_urls'] = file_urls
            item['document_names'] = document_names
            #item['IP_index'] = IP_index  # new

            #""" for IP rotation
            if self.init_valid_IPs is None:
                item['IP_index'] = [-1]
            else:
                n_half_valid_IPs = self.n_init_valid_IPs // 2
                valid_IPs_for_docs = self.init_valid_IPs[-n_half_valid_IPs:] if n_half_valid_IPs > 0 else self.init_valid_IPs
                item['IP_index'] = valid_IPs_for_docs
                print(
                    f'{self.n_init_valid_IPs - n_half_valid_IPs} IPs for data: {self.init_valid_IPs[0]} to {self.init_valid_IPs[-n_half_valid_IPs-1]}, current IP index: {IP_index}, {n_half_valid_IPs} IPs for docs: {valid_IPs_for_docs[0]} to {valid_IPs_for_docs[-1]}')
            # """ # end of IP rotation

            cookies = driver.get_cookies()
            print("cookies:", cookies) if PRINT else None
            item['session_cookies'] = cookies
            return item

        time.sleep(10)
        #panel_body = driver.find_element(By.XPATH, '/html/body/div[1]/div[2]/div[2]/div[2]')
        panel_body = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '/html/body/div[1]/div[2]/div[2]/div[2]')))
        document_table = panel_body.find_element(By.XPATH, '//*[@id="documents"]')

        n_documents_str = panel_body.find_element(By.XPATH, '//*[@id="documents_info"]').text.strip()
        n_documents_str = n_documents_str.split('of')[1]
        n_documents = int(re.search(r"\d+", n_documents_str).group())
        print(f'\nTotal documents: {n_documents}') if PRINT else None
        if n_documents > 0:
            if n_documents > 10:
                selection_input = driver.find_element(By.XPATH, '//*[@id="documents_length"]/label/select')
                select = Select(selection_input)
                select.select_by_visible_text("100")
                time.sleep(3)
                assert n_documents <= 100

            columns = document_table.find_elements(By.XPATH, './thead/tr/th')
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

            document_items = document_table.find_elements(By.XPATH, './tbody/tr')
            file_urls, document_names = [], []
            for document_index, document_item in enumerate(document_items):
                file_url = document_item.find_element(By.XPATH, f'./td/a').get_attribute('href')
                #print(f'doc {document_index+1} url: {file_url}')
                file_urls.append(file_url)

                item_identity = file_url.split('.')[-1]  # extension such as .pdf
                document_name = f"uid={document_index+1}.{item_identity}"
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
                print(f"    Document {document_index+1}: {document_name}") if PRINT else None
                document_name = replace_invalid_characters(document_name)
                document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
            app_df.at['other_fields.n_documents'] = n_documents

        if n_documents > 0:
            item = create_item(driver, folder_name, file_urls, document_names)
            yield item

        #self.ending(app_df)

    # Wandsworth
    def get_next_parse_func(self, tab_name):
        if tab_name == 'Application Dates':
            next_parse_func = self.parse_date_item_Wandsworth
        elif tab_name == 'Application Checks':  # Islington
            next_parse_func = self.parse_checks_Islington
        elif tab_name == 'Application Meetings':
            next_parse_func = self.parse_date_item_Wandsworth
        elif tab_name == 'Application Constraints':
            next_parse_func = self.parse_constraint_Wandsworth
        elif tab_name == 'Application Site History':
            next_parse_func = self.parse_site_history_Wandsworth
        elif tab_name == 'Application Refusal Reasons':
            next_parse_func = self.parse_refusal_reasons_Wandsworth
        elif tab_name == 'Consultees Details':
            next_parse_func = self.parse_consultee_item_Wandsworth
        elif tab_name == 'Neighbours Details':
            next_parse_func = self.parse_neighbour_item_Wandsworth
        elif tab_name == 'Associated Documents':
            next_parse_func = self.parse_document_item_Wandsworth
        else:
            print(f'Unknown tab name: {tab_name}')
            assert 1 == 0
        return next_parse_func

    def parse_data_item_Wandsworth(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        print(f"parse_data_item_Wandsworth, scraper name: {scraper_name}")
        folder_name = self.setup_storage_path(app_df)

        try:
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="content"]')))
            #data_blocks = tab_panel.find_elements(By.XPATH, './div')
            data_blocks = tab_panel.find_elements(By.CLASS_NAME, 'dataview')
        except TimeoutException:
            print('*** *** *** This application is not available. *** *** ***')
            return

        items = []
        for data_block in data_blocks[1:-1]:
            items.extend(data_block.find_elements(By.XPATH, './ul/li/div'))

        n_items = len(items)
        print(f"\n1. Details Tab: {n_items} from {len(data_blocks)-2} blocks.")  # if PRINT else None #print(f"Main Details Tab: {n_items}")
        contact_dict = {}
        for i, item in enumerate(items):
            item_name = item.find_element(By.XPATH, './span').text.strip()
            if item_name == "Decision":
                item_name1 = item_name + ' (decision)'
                data_name = self.details_dict[item_name1]
                item_values = driver.execute_script("return arguments[0].lastChild.textContent;", item).strip().split('\n')

                app_df.at[data_name] = item_values[0].strip()
                print(f"<{item_name1}> scraped: {app_df.at[data_name]}") if PRINT else None

                item_name = item_name + ' (date)'
                data_name = self.details_dict[item_name]
                try:
                    item_value = item_values[1].strip()
                except IndexError:
                    item_value = ''
            elif item_name == "Location Co ordinates":
                item_name1 = item_name + ' (easting)'
                data_name = self.details_dict[item_name1]
                item_values = driver.execute_script("return arguments[0].lastChild.textContent;", item).strip()
                """
                item_values = item_values.split('\xa0')
                try:
                    app_df.at[data_name] = item_values[1].strip()
                except IndexError:
                    app_df.at[data_name] = ''
                print(f"<{item_name1}> scraped: {app_df.at[data_name]}") if PRINT else None

                item_name = item_name + ' (northing)'
                data_name = self.details_dict[item_name]
                try:
                    item_value = item_values[3].strip()
                except IndexError:
                    item_value = ''
                """
                item_values = re.findall(r'\d+', item_values)
                try:
                    app_df.at[data_name] = item_values[0].strip()
                except IndexError:
                    app_df.at[data_name] = ''
                print(f"<{item_name1}> scraped: {app_df.at[data_name]}") if PRINT else None

                item_name = item_name + ' (northing)'
                data_name = self.details_dict[item_name]
                try:
                    item_value = item_values[1].strip()
                except IndexError:
                    item_value = ''
            else:
                data_name = self.details_dict[item_name]
                item_value = driver.execute_script("return arguments[0].lastChild.textContent;", item).strip()
            # if data_name in self.app_dfs.columns:
            try:
                if data_name in ['Tel'] and len(item_value) > 0:
                    officer_names = re.findall(r'\D+', item_value)
                    officer_name = ' '.join(officer_names).strip()
                    contact_dict['Officer'] = [officer_name]
                    digits = re.findall(r'\d+', item_value)
                    officer_tel = ''.join(digits)
                    contact_dict[data_name] = [officer_tel]
                    print(f"<{data_name}> (contact) scraped: {officer_name}, {officer_tel}") if PRINT else None
                    contact_df = pd.DataFrame(contact_dict)
                    contact_df.to_csv(f"{self.data_storage_path}{folder_name}/contact.csv", index=False)
                else:
                    app_df.at[data_name] = item_value
                    print(f"<{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
            # New or contact
            except KeyError:
                app_df[data_name] = item_value
                print(f"<{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None

        app_df = self.set_default_items(app_df)
        tabs = data_blocks[-1].find_elements(By.XPATH, './ul/li/div/a')
        tab_names = [tab.text.strip() for tab in tabs]
        tab_urls = [tab.get_attribute('href') for tab in tabs]
        # Add document tab
        tab_names.append('Associated Documents')
        footerlinks = tab_panel.find_elements(By.CLASS_NAME, 'FooterLinks')
        print('number of footerlinks: ', len(footerlinks))
        for footerlink in footerlinks:
            footerlink_text = footerlink.text.strip()
            if 'Documents' in footerlink_text:
                tab_urls.append(footerlink.get_attribute('href'))
                break

        next_parse_func = self.get_next_parse_func(tab_names[0])
        yield SeleniumRequest(url=tab_urls[0], callback=next_parse_func, meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names, 'tab_urls': tab_urls})

    def parse_date_item_Wandsworth(self, response):
        app_df, folder_name, tab_names, tab_urls = self.parse_date_item(response)

        next_parse_func = self.get_next_parse_func(tab_names[1])
        yield SeleniumRequest(url=tab_urls[1], callback=next_parse_func, meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names[1:], 'tab_urls': tab_urls[1:]})

    def scrape_for_csv(self, response, tab_panel_path='//*[@id="content"]', item_keyword='Reference', csv_name='constraint'):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        driver = response.request.meta["driver"]
        tab_names = response.meta['tab_names']
        tab_urls = response.meta['tab_urls']

        tab_panel = driver.find_element(By.XPATH, tab_panel_path)
        data_blocks = tab_panel.find_elements(By.CLASS_NAME, 'dataview')

        items = []
        for data_block in data_blocks[1:-1]:
            items.extend(data_block.find_elements(By.XPATH, './ul/li/div'))

        n_contents = 0
        content_dict = {}
        if csv_name == 'neighbours details':
            item_name = data_blocks[1].find_element(By.XPATH, './h1').text.strip()
            n_contents = len(items)
            for i, item in enumerate(items):
                item_value = item.text.strip()
                try:
                    content_dict[item_name].append(item_value)
                except KeyError:
                    content_dict[item_name] = [item_value]
        else:
            for i, item in enumerate(items):
                item_name = item.find_element(By.XPATH, './span').text.strip()
                item_value = driver.execute_script("return arguments[0].lastChild.textContent;", item).strip()
                if item_name == item_keyword:
                    n_contents += 1
                try:
                    content_dict[item_name].append(item_value)
                except KeyError:
                    content_dict[item_name] = [item_value]
        if n_contents > 0:
            content_df = pd.DataFrame(content_dict)
            content_df.to_csv(f"{self.data_storage_path}{folder_name}/{csv_name}.csv", index=False)
        return app_df, folder_name, tab_names, tab_urls, n_contents

    def parse_constraint_Wandsworth(self, response):
        """
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        driver = response.request.meta["driver"]
        tab_names = response.meta['tab_names']
        tab_urls = response.meta['tab_urls']

        tab_panel = driver.find_element(By.XPATH, '//*[@id="content"]')
        data_blocks = tab_panel.find_elements(By.CLASS_NAME, 'dataview')

        items = []
        for data_block in data_blocks[1:-1]:
            items.extend(data_block.find_elements(By.XPATH, './ul/li/div'))

        n_constraints = 0
        constraint_dict = {}
        for i, item in enumerate(items):
            item_name = item.find_element(By.XPATH, './span').text.strip()
            item_value = driver.execute_script("return arguments[0].lastChild.textContent;", item).strip()
            if item_name == 'Reference':
                n_constraints += 1
            try:
                constraint_dict[item_name].append(item_value)
            except KeyError:
                constraint_dict[item_name] = [item_value]

        app_df.at['other_fields.n_constraints'] = n_constraints
        print(f"\nConstraint Tab: {n_constraints} constraints.")
        if n_constraints > 0:
            constraint_df = pd.DataFrame(constraint_dict)
            constraint_df.to_csv(f"{self.data_storage_path}{folder_name}/constraint.csv", index=False)
        """
        app_df, folder_name, tab_names, tab_urls, n_constraints = \
            self.scrape_for_csv(response, tab_panel_path='//*[@id="content"]', item_keyword='Reference', csv_name='constraint')
        app_df.at['other_fields.n_constraints'] = n_constraints
        print(f"\n{tab_names[0]} Tab: {n_constraints} constraints.")

        next_parse_func = self.get_next_parse_func(tab_names[1])
        yield SeleniumRequest(url=tab_urls[1], callback=next_parse_func, meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names[1:], 'tab_urls': tab_urls[1:]})

    def parse_site_history_Wandsworth(self, response):
        app_df, folder_name, tab_names, tab_urls, n_site_histories = \
            self.scrape_for_csv(response, tab_panel_path='//*[@id="content"]', item_keyword='Application Number', csv_name='site history')
        #app_df.at['other_fields.n_site_history'] = n_site_histories
        print(f"\n{tab_names[0]} Tab: {n_site_histories} site histories.")

        next_parse_func = self.get_next_parse_func(tab_names[1])
        yield SeleniumRequest(url=tab_urls[1], callback=next_parse_func, meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names[1:], 'tab_urls': tab_urls[1:]})
    # specific
    def parse_refusal_reasons_Wandsworth(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        driver = response.request.meta["driver"]
        tab_names = response.meta['tab_names']
        tab_urls = response.meta['tab_urls']

        tab_panel = driver.find_element(By.XPATH, '//*[@id="content"]')
        data_blocks = tab_panel.find_elements(By.CLASS_NAME, 'dataview')

        items = data_blocks[2:-1]
        n_contents = len(items)
        print(f"\n{tab_names[0]} Tab: {n_contents} refusal reasons.")
        content_dict = {}
        item_name = 'Reasons'
        for i, item in enumerate(items):
            #item_name = item.find_element(By.XPATH, './h1').text.strip()
            item_value = item.find_element(By.XPATH, './div/p').text.strip()
            try:
                content_dict[item_name].append(item_value)
            except KeyError:
                content_dict[item_name] = [item_value]
        if n_contents > 0:
            content_df = pd.DataFrame(content_dict)
            content_df.to_csv(f"{self.data_storage_path}{folder_name}/refusal reasons.csv", index=False)

        next_parse_func = self.get_next_parse_func(tab_names[1])
        yield SeleniumRequest(url=tab_urls[1], callback=next_parse_func, meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names[1:], 'tab_urls': tab_urls[1:]})

    def parse_consultee_item_Wandsworth(self, response):
        app_df, folder_name, tab_names, tab_urls, n_consultees = \
            self.scrape_for_csv(response, tab_panel_path='//*[@id="content"]', item_keyword='Name', csv_name='consultees details')
        app_df.at['other_fields.n_comments_consultee_responded'] = n_consultees
        app_df.at['other_fields.n_comments'] += n_consultees
        print(f"\n{tab_names[0]} Tab: {n_consultees} consultees.")

        next_parse_func = self.get_next_parse_func(tab_names[1])
        yield SeleniumRequest(url=tab_urls[1], callback=next_parse_func, meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names[1:], 'tab_urls': tab_urls[1:]})

    def parse_neighbour_item_Wandsworth(self, response):
        app_df, folder_name, tab_names, tab_urls, n_neighbours = \
            self.scrape_for_csv(response, tab_panel_path='//*[@id="content"]', item_keyword='Reference', csv_name='neighbours details')
        app_df.at['other_fields.n_comments_public_received'] = n_neighbours
        app_df.at['other_fields.n_comments'] += n_neighbours
        print(f"\n{tab_names[0]} Tab: {n_neighbours} neighbours.")

        next_parse_func = self.get_next_parse_func(tab_names[1])
        yield SeleniumRequest(url=tab_urls[1], callback=next_parse_func, meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names[1:], 'tab_urls': tab_urls[1:]})

    # Discard on Oct 23
    def parse_document_item_Wandsworth2(self, response):
        def create_item(driver, folder_name, file_urls, document_names):
            if not os.path.exists(self.failed_downloads_path + folder_name):
                os.mkdir(self.failed_downloads_path + folder_name)

            item = DownloadFilesItem()
            item['file_urls'] = file_urls
            item['document_names'] = document_names

            cookies = driver.get_cookies()
            print("cookies:", cookies) if PRINT else None
            item['session_cookies'] = cookies
            return item

        def get_documents(n_documents, file_urls, document_names, document_type, folder_name):
            document_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="gvResults"]')))
            document_items = document_list.find_elements(By.XPATH, './tbody/tr')[1:]
            for document_item in document_items:
                n_documents += 1
                file_url = document_item.find_element(By.XPATH, f'./td[3]/a').get_attribute('href')
                file_urls.append(file_url)

                document_name = f"uid={n_documents}.pdf"
                document_description = document_item.find_element(By.XPATH, './td[2]').text.strip()
                document_name = f"desc={document_description}&{document_name}"
                document_name = f"type={document_type}&{document_name}"
                document_date = document_item.find_element(By.XPATH, './td[1]').text.strip()
                document_name = f"date={document_date}&{document_name}"

                print(f"    Document {n_documents}: {document_name}") if PRINT else None
                document_name = replace_invalid_characters(document_name)
                document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
            return n_documents, file_urls, document_names

        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        driver = response.request.meta["driver"]
        tab_names = response.meta['tab_names']
        tab_urls = response.meta['tab_urls']
        try:
            type_index, n_types = response.meta['type_index'], response.meta['n_type']
        except KeyError:
            type_index, n_types = 0, 0

        document_panel = driver.find_element(By.XPATH, '//*[@id="form1"]/div[3]')
        table = document_panel.find_element(By.XPATH, '//*[@id="gvDocs"]')
        if type_index == 0:
            n_documents, file_urls, document_names = 0, [], []
            sub_tables = table.find_elements(By.XPATH, './tbody/tr')[1:]
            n_types = len(sub_tables)

            if n_types > 0:
                type_index = n_types

        if type_index > 0:
            sub_table = document_panel.find_element(By.XPATH, f'//*[@id="gvDocs"]/tbody/tr[{n_types-type_index+2}]')
            document_type = sub_table.find_element(By.XPATH, './td[1]/span').text.strip()
            document_count = int(sub_table.find_element(By.XPATH, './td[2]/span').text.strip())
            document_tab = sub_table.find_element(By.XPATH, './td[3]/a')
            print(f"[{type_index} / {n_types}]. Document type <{document_type}> has {document_count} documents:")
            document_tab.click()
            n_documents, file_urls, document_names = get_documents(n_documents, file_urls, document_names, document_type, folder_name)

            if type_index == n_types:
                print('compelted')
                app_df.at['other_fields.n_documents'] = n_documents
                print(f'Total documents: {n_documents}') if PRINT else None
                item = create_item(driver, folder_name, file_urls, document_names)
                yield item
            else:
                print('continue')
                yield SeleniumRequest(url=tab_urls[0], callback=self.parse_document_item_Wandsworth,
                                      meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names, 'tab_urls': tab_urls, 'type_index': type_index+1, 'n_types': n_types})
        if type_index == n_types:
            self.ending(app_df)

    def parse_document_item_Wandsworth(self, response):
        def create_item(driver, folder_name, file_urls, document_names):
            if not os.path.exists(self.failed_downloads_path + folder_name):
                os.mkdir(self.failed_downloads_path + folder_name)

            item = DownloadFilesItem()
            item['file_urls'] = file_urls
            item['document_names'] = document_names

            cookies = driver.get_cookies()
            print("cookies:", cookies) if PRINT else None
            item['session_cookies'] = cookies
            return item

        def get_documents(n_documents, file_urls, document_names, document_type, folder_name):
            document_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="gvResults"]')))
            document_items = document_list.find_elements(By.XPATH, './tbody/tr')[1:]
            for document_item in document_items:
                n_documents += 1
                file_url = document_item.find_element(By.XPATH, f'./td[3]/a').get_attribute('href')

                uid = file_url.split('=')[-1]
                file_url = f'https://planning2.wandsworth.gov.uk/iam/IAMCache/{uid}/{uid}.pdf'

                print(file_url)
                # https://planning2.wandsworth.gov.uk/IAM/IAMLink.aspx?docid=439852
                # https://planning2.wandsworth.gov.uk/iam/IAMCache/439852/439852.pdf
                file_urls.append(file_url)

                document_name = f"uid={n_documents}.pdf"
                document_description = document_item.find_element(By.XPATH, './td[2]').text.strip()
                document_name = f"desc={document_description}&{document_name}"
                document_name = f"type={document_type}&{document_name}"
                document_date = document_item.find_element(By.XPATH, './td[1]').text.strip()
                document_name = f"date={document_date}&{document_name}"

                print(f"    Document {n_documents}: {document_name}") if PRINT else None
                document_name = replace_invalid_characters(document_name)
                document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
            return n_documents, file_urls, document_names

        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        driver = response.request.meta["driver"]

        document_panel = driver.find_element(By.XPATH, '//*[@id="form1"]/div[3]')
        table = document_panel.find_element(By.XPATH, '//*[@id="gvDocs"]')
        n_documents, file_urls, document_names = 0, [], []

        sub_tables = table.find_elements(By.XPATH, './tbody/tr')[1:]
        n_sub_tables = len(sub_tables)
        for i in range(n_sub_tables):
            sub_table = driver.find_element(By.XPATH, f'//*[@id="gvDocs"]/tbody/tr[{i+2}]')
            document_type = sub_table.find_element(By.XPATH, './td[1]/span').text.strip()
            document_count = int(sub_table.find_element(By.XPATH, './td[2]/span').text.strip())
            document_tab = sub_table.find_element(By.XPATH, './td[3]/a')
            print(f"Document type <{document_type}> has {document_count} documents:")
            document_tab.click()
            n_documents, file_urls, document_names = get_documents(n_documents, file_urls, document_names, document_type, folder_name)

        app_df.at['other_fields.n_documents'] = n_documents
        print(f'Total documents: {n_documents}') if PRINT else None

        if n_documents > 0:
            item = create_item(driver, folder_name, file_urls, document_names)
            yield item

        self.ending(app_df)


     # Camden
    def parse_data_item_Camden(self, response):
        pass


    # Merton
    def parse_data_item_Merton(self, response):
        pass


    # Islington
    def parse_data_item_Islington(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        print(f"parse_data_item_Wandsworth, scraper name: {scraper_name}")
        folder_name = self.setup_storage_path(app_df)

        try:
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="Template"]')))
            #data_blocks = tab_panel.find_elements(By.XPATH, './div')
            data_blocks = tab_panel.find_elements(By.CLASS_NAME, 'dataview')
        except TimeoutException:
            print('*** *** *** This application is not available. *** *** ***')
            return

        items = []
        for data_block in data_blocks[1:-1]:
            items.extend(data_block.find_elements(By.XPATH, './ul/li/div'))

        n_items = len(items)
        print(f"\n1. Details Tab: {n_items} from {len(data_blocks)-2} blocks.")  # if PRINT else None #print(f"Main Details Tab: {n_items}")
        contact_dict = {}
        for i, item in enumerate(items):
            item_name = item.find_element(By.XPATH, './span').text.strip()
            if item_name == "Decision":
                item_name1 = item_name + ' (decision)'
                data_name = self.details_dict[item_name1]
                item_values = driver.execute_script("return arguments[0].lastChild.textContent;", item).strip().split('\n')

                app_df.at[data_name] = item_values[0].strip()
                print(f"<{item_name1}> scraped: {app_df.at[data_name]}") if PRINT else None

                item_name = item_name + ' (date)'
                data_name = self.details_dict[item_name]
                try:
                    item_value = item_values[1].strip()
                except IndexError:
                    item_value = ''
            elif item_name == "Location Co ordinates":
                item_name1 = item_name + ' (easting)'
                data_name = self.details_dict[item_name1]
                item_values = driver.execute_script("return arguments[0].lastChild.textContent;", item).strip()
                """
                item_values = item_values.split('\xa0')
                try:
                    app_df.at[data_name] = item_values[1].strip()
                except IndexError:
                    app_df.at[data_name] = ''
                print(f"<{item_name1}> scraped: {app_df.at[data_name]}") if PRINT else None

                item_name = item_name + ' (northing)'
                data_name = self.details_dict[item_name]
                try:
                    item_value = item_values[3].strip()
                except IndexError:
                    item_value = ''
                """
                item_values = re.findall(r'\d+', item_values)
                try:
                    app_df.at[data_name] = item_values[0].strip()
                except IndexError:
                    app_df.at[data_name] = ''
                print(f"<{item_name1}> scraped: {app_df.at[data_name]}") if PRINT else None

                item_name = item_name + ' (northing)'
                data_name = self.details_dict[item_name]
                try:
                    item_value = item_values[1].strip()
                except IndexError:
                    item_value = ''
            else:
                data_name = self.details_dict[item_name]
                item_value = driver.execute_script("return arguments[0].lastChild.textContent;", item).strip()
            # if data_name in self.app_dfs.columns:
            try:
                if data_name in ['Tel'] and len(item_value) > 0:
                    officer_names = re.findall(r'\D+', item_value)
                    officer_name = ' '.join(officer_names).strip()
                    contact_dict['Officer'] = [officer_name]
                    digits = re.findall(r'\d+', item_value)
                    officer_tel = ''.join(digits)
                    contact_dict[data_name] = [officer_tel]
                    print(f"<{data_name}> (contact) scraped: {officer_name}, {officer_tel}") if PRINT else None
                    contact_df = pd.DataFrame(contact_dict)
                    contact_df.to_csv(f"{self.data_storage_path}{folder_name}/contact.csv", index=False)
                else:
                    app_df.at[data_name] = item_value
                    print(f"<{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
            # New or contact
            except KeyError:
                app_df[data_name] = item_value
                print(f"<{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None

        app_df = self.set_default_items(app_df)
        tabs = data_blocks[-1].find_elements(By.XPATH, './ul/li/div/a')
        tab_names = [tab.text.strip() for tab in tabs]
        tab_urls = [tab.get_attribute('href') for tab in tabs]

        # Add document tab
        """
        tab_names.append('Associated Documents')
        footerlinks = tab_panel.find_elements(By.CLASS_NAME, 'FooterLinks')
        print('number of footerlinks: ', len(footerlinks))
        for footerlink in footerlinks:
            footerlink_text = footerlink.text.strip()
            if 'Documents' in footerlink_text:
                tab_urls.append(footerlink.get_attribute('href'))
                break
        """

        next_parse_func = self.get_next_parse_func(tab_names[0])
        yield SeleniumRequest(url=tab_urls[0], callback=next_parse_func, meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names, 'tab_urls': tab_urls})

    def parse_checks_Islington(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        driver = response.request.meta["driver"]
        tab_names = response.meta['tab_names']
        tab_urls = response.meta['tab_urls']

        next_parse_func = self.get_next_parse_func(tab_names[1])
        yield SeleniumRequest(url=tab_urls[1], callback=next_parse_func,
                              meta={'app_df': app_df, 'folder_name': folder_name, 'tab_names': tab_names[1:], 'tab_urls': tab_urls[1:]})
