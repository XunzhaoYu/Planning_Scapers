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



class Tascomi_Scraper(scrapy.Spider):
    name = 'Tascomi_Scraper'

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
            auth_names = get_scraper_by_type('Tascomi')  # Your_Scraper_Type
            auth_names = [auth_name for auth_name in auth_names if not auth_name.startswith('.')]
            auth_names.sort(key=str.lower)
            print(len(auth_names), auth_names)

            app_dfs = []
            self.auth_index = 7
            # 0 'Barking', http://paplan.lbbd.gov.uk/online-applications/applicationDetails.do?activeTab=summary&keyVal=0100002PRIOR
            #   [early years unavailable & reChapcha] https://online-befirst.lbbd.gov.uk/planning/index.html?fa=getApplication&id=9134
            # 1 'Ceredigion', https://ceredigion-online.tascomi.com/planning/index.html?fa=getApplication&id=36520
            # 2 'Coventry', https://planningsearch.coventry.gov.uk/planning/application/556153 [X]
            #   [Search ID] https://planandregulatory.coventry.gov.uk/planning/index.html?fa=getApplication&id=249328
            # 3 'Dartmoor', https://dartmoor-online.tascomi.com/planning/index.html?fa=getApplication&id=126932
            # 4 'Gwynedd', https://amg.gwynedd.llyw.cymru/planning/index.html?fa=getApplication&id=9310
            # 5 'Hackney', https://developmentandhousing.hackney.gov.uk/planning/index.html?fa=getApplication&id=63606
            # 6 'Harrow', https://planningsearch.harrow.gov.uk/planning/planning-application?RefType=GFPlanning&KeyNo=696420
            #   [404 Not Found, Need Search ID] https://planningsearch.harrow.gov.uk/planning/index.html?fa=getApplication&id=131697
            # 7 'Liverpool', https://lar.liverpool.gov.uk/planning/index.html?fa=getApplication&id=137542
            # 8 'NewcastleUponTyne', https://portal.newcastle.gov.uk/planning/index.html?fa=getApplication&id=73907
            # 9 'WalthamForest', https://builtenvironment.walthamforest.gov.uk/planning/index.html?fa=getApplication&id=23739
            # 10 'Warrington',
            #   [different] https://online.warrington.gov.uk/planning/index.html?fa=getApplication&id=167271
            # 11 'Wirral', https://online.wirral.gov.uk/planning/index.html?fa=getApplication&id=160647
            self.year = -1
            self.auth = auth_names[int(self.auth_index)]
            self.data_storage_path, self.data_upload_path = initialize_paths(self.data_storage_path, self.auth, self.year)

            sample_index = 4
            for auth in auth_names[self.auth_index:self.auth_index + 1]:
                # for year in years:
                #    file_path = f"{get_list_storage_path()}{auth}/{auth}{year}.csv"
                filenames = get_filenames(f"{get_list_storage_path()}{auth}/")
                print(f"{auth}. number of files: {len(filenames)}")
                for filename in filenames[5:6]:
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

        """ for IP rotation
        valid_IP_proxy_path = f'{Path(get_project_root()).parent}/valid_IPs/{self.auth}.csv'
        if os.path.exists(valid_IP_proxy_path):
            valid_IPs_df = pd.read_csv(valid_IP_proxy_path)
            self.init_valid_IPs = valid_IPs_df.iloc[:, 0].tolist()
            self.n_init_valid_IPs = len(self.init_valid_IPs)
        else:
            self.init_valid_IPs = None
            self.n_init_valid_IPs = 0
        # """ # end of IP rotation

        self.parse_func = self.parse_data_item_Tascomi
        """
        if auth_names[self.auth_index] in ['Wandsworth']:
            self.parse_func = self.parse_data_item_Wandsworth
        elif auth_names[self.auth_index] in ['Camden']:
            self.parse_func = self.parse_data_item_Camden
        elif auth_names[self.auth_index] in ['Merton']:
            self.parse_func = self.parse_data_item_Merton
        elif auth_names[self.auth_index] in ['Islington']:
            self.parse_func = self.parse_data_item_Islington
        #"""


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(Tascomi_Scraper, cls).from_crawler(crawler, *args, **kwargs)  # Name_Your_Scraper
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
            yield SeleniumRequest(url=url, callback=self.parse_func, meta={'app_df': app_df})
            #yield SeleniumRequest(url=url, callback=self.parse_func, meta={'app_df': app_df, 'valid_IPs': self.init_valid_IPs})
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
            return f"{day}-{month}-{year}"
        else:
            return date_string

    details_dict = {'Application Reference Number': 'uid',
                    'Application Type': 'other_fields.application_type',
                    'Proposal': 'description',

                    'Applicant': 'other_fields.applicant_name',
                    'Agent': 'other_fields.agent_name',
                    'Location': 'address',
                    'Grid Reference': 'other_fields.grid_reference', # New
                    'Ward': 'other_fields.ward_name',
                    'Parish / Community': 'other_fields.parish',
                    'Officer': 'other_fields.case_officer',
                    'Decision Level': 'other_fields.expected_decision_level',
                    'Application Status': 'other_fields.status',

                    'Received Date': 'other_fields.date_received',
                    'Valid Date': 'other_fields.date_validated',
                    'Expiry Date': 'other_fields.application_expires_date',

                    'Extension Of Time': 'other_fields.extension_of_time', # New
                    'Extension Of Time Due Date': 'other_fields.extension_of_time_due_date', # New
                    'Planning Performance Agreement': 'other_fields.planning_performance_agreement', # New
                    'Planning Performance Agreement Due Date': 'other_fields.planning_performance_agreement_due_date', # New
                    'Proposed Committee Date': 'other_fields.proposed_meeting_date', # New
                    'Actual Committee Date': 'other_fields.meeting_date',
                    'Decision Issued Date': 'other_fields.decision_issued_date',
                    'Decision': 'other_fields.decision',
                    # Appeal
                    'Appeal Reference': 'other_fields.appeal_reference',
                    'Appeal Status': 'other_fields.appeal_status',
                    'Appeal External Decision': 'other_fields.appeal_result',
                    'Appeal External Decision Date': 'other_fields.appeal_decision_date',
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



    def scrape_data_items(self, app_df, items, item_values):
        for item, value in zip(items, item_values):
            item_name = item.text.strip()[:-1]
            print('item_name:', item_name)
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

    # Start_Your_Scraper # Wiltshire,
    # 1. Dynamic element id. 2. Need WebDriverWait
    def parse_data_item_Tascomi(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        print(f"parse_data_item_Tascomi, scraper name: {scraper_name}")
        folder_name = self.setup_storage_path(app_df)

        try:
            tab_panel = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="application_details"]/div/div[2]')))
        except TimeoutException:  # ***** situation to be completed: application not available
            # Planning Application details not available . e.g. auth=123, year=[21:]
            note = response.xpath('//*[@id="pageheading"]/h1/text()').get()
            print('note: ', note)
            return
            """
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
            """
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  # scroll down to the bottom of this page.

        # --- --- --- Main Details --- --- ---
        detail_list = tab_panel.find_elements(By.XPATH, './div/div')
        items = [detail.find_element(By.XPATH, './div[1]') for detail in detail_list]
        item_values = [detail.find_element(By.XPATH, './div[2]') for detail in detail_list]
        n_items = len(items)
        """
        if n_items == 0:  # reload
            print('reload ...')
            yield SeleniumRequest(url=driver.current_url, callback=self.parse_func, meta={'app_df': app_df})
            return
        if n_items == 0:
            assert 0 == 1
        """
        print(f"\n1. Details Tab: {n_items}")  # if PRINT else None
        app_df = self.scrape_data_items(app_df, items, item_values)

        # --- --- --- Associate Documents --- --- ---
        try:
            doc_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@id="documents"]/div/div[2]/table/tbody')))
            document_items = doc_list.find_elements(By.XPATH, './tr')
            n_documents = len(document_items)
            app_df.at['other_fields.n_documents'] = n_documents
            print(f"\n2 Documents Tab: {n_documents}")
        except TimeoutException:
            app_df.at['other_fields.n_documents'] = 0
            print(f"\n2 Documents Tab: No Document")
        if n_documents > 0:
            n_documents, file_urls, document_names = 0, [], []
            for document_item in document_items:
                n_documents += 1
                print(f'    - - - Document {n_documents} - - -') if PRINT else None
                columns = document_item.find_elements(By.XPATH, './td')
                assert len(columns) == 5
                file_url = columns[4].find_element(By.XPATH, './a').get_attribute('href')
                print(file_url) if PRINT else None
                file_urls.append(file_url)

                document_type = columns[0].text.strip()
                #print(document_type)
                document_description = columns[1].text.strip()
                if len(document_description) > 40:
                    document_description = document_description.split('.')[0][:40]
                print(document_description) if PRINT else None
                document_date = columns[3].text.strip()
                #print(document_date)
                document_name = f'date={document_date}&type={document_type}&desc={document_description}&uid={n_documents}'
                print(document_name) if PRINT else None

                document_name = replace_invalid_characters(document_name)
                # print('new: ', document_name) if PRINT else None
                document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
            item = self.create_item(driver, folder_name, file_urls, document_names)
            yield item

        #assert 1 == 0
        self.ending(app_df)