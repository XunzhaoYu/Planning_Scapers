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




class CCED_Scraper(scrapy.Spider):
    name = 'CCED_Scraper'  # Similar to Atrium, but has different document system (FormRequest + payloads)

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
            auth_names = get_scraper_by_type('CCED')  # CCED
            auth_names = [auth_name for auth_name in auth_names if not auth_name.startswith('.')]
            auth_names.sort(key=str.lower)
            print(len(auth_names), auth_names)

            app_dfs = []
            self.auth_index = 1 # ['Christchurch' (need IP rotation), 'DorsetCouncil']
            self.year = -1
            self.auth = auth_names[int(self.auth_index)]
            self.data_storage_path, self.data_upload_path = initialize_paths(self.data_storage_path, self.auth, self.year)

            sample_index = 6
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

        self.parse_func = self.parse_data_item_DorsetCouncil
        #if auth_names[self.auth_index] in ['Christchurch']:
        #    self.parse_func = self.parse_data_item_Christchurch



    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CCED_Scraper, cls).from_crawler(crawler, *args, **kwargs)
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
            return f"{year}-{month}-{day}"
        else:
            return date_string

    details_dict = {# Main Details
                    'Application No': 'uid',
                    'Type': 'other_fields.application_type',
                    'Case Officer': 'other_fields.case_officer',

                    'Committee / Delegated': 'other_fields.expected_decision_level',
                    'Status': 'other_fields.status',
                    'Committee Date': 'other_fields.meeting_date',
                    'Proposal': 'description',

                    'Valid Date': 'other_fields.date_validated',
                    'Decision': 'other_fields.decision',
                    'Issue Date': 'other_fields.decision_issued_date',
                    'Consultation End': 'other_fields.consultation_end_date',  # Christchurch only
                    'Neighbour Expiry': 'other_fields.neighbour_expiry',  # new: DorsetCouncil only.
                    'Authority': 'other_authority',  # new
                    # Location:
                    'Address': 'address',
                    'Easting': 'other_fields.easting',
                    'Northing': 'other_fields.northing',
                    'Ward': 'other_fields.ward_name',
                    'Parish': 'other_fields.parish',

                    'Ward members': 'Ward Members',  # A link.
                    # Appeals:
                    'Appeal_Number': 'other_fields.appeal_reference',
                    'Appeal_PI ref': 'other_fields.appeal_PI_reference',  # new
                    'Appeal_Method': 'other_fields.appeal_method',
                    'Appeal_Start date': 'other_fields.appeal_start_date',
                    'Appeal_Comment to PINS by': 'other_fields.appeal_comment_to_PINS_by',  # new
                    'Appeal_Inquiry Date': 'other_fields.appeal_inquiry_date',  # new
                    'Appeal_Venue': 'other_fields.appeal_venue',  # new
                    'Appeal_Decision': 'other_fields.appeal_result',
                    'Appeal_Issue Date': 'other_fields.appeal_decision_date'
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
        app_df['other_fields.n_comments_public_received'] = 0
        app_df['other_fields.n_comments_public_objections'] = 'Not available'
        app_df['other_fields.n_comments_public_supporting'] = 'Not available'
        app_df['other_fields.n_comments_consultee_total_consulted'] = 'Not available'
        app_df['other_fields.n_comments_consultee_responded'] = 0
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

    def scrape_for_csv_single(self, csv_name, column_name, table_items, folder_name, path='td'):
        content_dict = {column_name: [table_item.find_element(By.XPATH, f'./{path}').text.strip() for table_item in table_items]}
        content_df = pd.DataFrame(content_dict)
        content_df.to_csv(f"{self.data_storage_path}{folder_name}/{csv_name}.csv", index=False)

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

    # With Payload.
    def create_item(self, driver, folder_name, file_urls, document_names, payloads):
        if not os.path.exists(self.failed_downloads_path + folder_name):
            os.mkdir(self.failed_downloads_path + folder_name)

        item = DownloadFilesItem()
        item['file_urls'] = file_urls
        item['document_names'] = document_names
        item['payloads'] = payloads

        cookies = driver.get_cookies()
        print("cookies:", cookies) if PRINT else None
        item['session_cookies'] = cookies
        return item

    # Christchurch:
    def parse_data_item_Christchurch(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        try:
            if 'Disclaimer' in response.xpath('//*[@id="aspnetForm"]/div[3]/h2[1]/text()').get():
                print('Click: Accept.')
                driver.find_element(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_btnAccept"]').click()
        except TypeError:
            pass
        print(f"parse_data_item_Christchurch scraper name: {scraper_name}")

        folder_name = self.setup_storage_path(app_df)



    # DorsetCouncil: https://planning.dorsetcouncil.gov.uk/plandisp.aspx?recno=393413
    def parse_data_item_DorsetCouncil(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        try:
            disclaimer_index = 2 if scraper_name == 'DorsetCouncil' else 1

            if 'Disclaimer' in response.xpath(f'//*[@id="aspnetForm"]/div[3]/h2[{disclaimer_index}]/text()').get():
                print('Click: Accept.')
                driver.find_element(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_btnAccept"]').click()
        except TypeError:
            pass
        print(f"parse_data_item_DorsetCouncil, scraper name: {scraper_name}")

        folder_name = self.setup_storage_path(app_df)

        try:
            # --- --- --- Main Details --- --- ---
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="planningdetails_wrapper"]')))
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
        detail_blocks = tab_panel.find_elements(By.XPATH, './div')

        items, item_values = [], []
        for detail_block in detail_blocks:
            items.extend(detail_block.find_elements(By.XPATH, './span'))
            item_values.extend(detail_block.find_elements(By.XPATH, './p'))
        n_items = len(items)
        print(f"\n1. Main Details Tab: {n_items} items from {len(detail_blocks)} blocks.")
        app_df = self.scrape_data_items(app_df, items[:-1], item_values[:-1])  # Exclude the last one: ' '

        tabs = driver.find_elements(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_RadTabStrip1"]/div/ul/li/a')
        app_df = self.set_default_items(app_df)
        for tab_index, tab in enumerate(tabs[1:]):
            tab.click()
            tab_name = tab.text.strip()
            # print(f"tab {tab_index + 1}: {tab_name}")
            # --- --- --- Location --- --- ---
            if 'Location' in tab_name:
                location_blocks = driver.find_elements(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_pvLocation"]/div')
                items, item_values = [], []
                for location_block in location_blocks:
                    items.extend(location_block.find_elements(By.XPATH, './span'))
                    item_values.extend(location_block.find_elements(By.XPATH, './p'))
                n_items = len(items)
                print(f"\n{tab_index+2}. {tab_name} Tab: {n_items} items.")
                for item, value in zip(items, item_values):
                    item_name = item.text.strip()
                    data_name = self.details_dict[item_name]
                    item_value = value.text.strip()
                    try:
                        if data_name in ['Ward Members']:
                            if item_value == 'Find out who your ward councillor is':
                                print(f"    <{item_name}> scraped (ward members): None.") if PRINT else None
                            else:
                                ward_member_strings = item_value.split('\n')
                                print(f'    {ward_member_strings}')
                                ward_member_names = []
                                for member_index in range(len(ward_member_strings)):
                                    if len(ward_member_strings[member_index]) > 0:
                                        ward_member_name = ward_member_strings[member_index].split('Cllr')[1].strip()
                                        print(f'        Ward Member {member_index+1}: {ward_member_name}')
                                        ward_member_names.append(ward_member_name)
                                    else:
                                        break
                                pd.DataFrame({'Ward Members': ward_member_names}).to_csv(f"{self.data_storage_path}{folder_name}/ward_members.csv", index=False)
                                """
                                    ward_url = value.find_element(By.XPATH, './a').get_attribute('href')
                                    print(ward_url)
                                    assert ward_url == 'https://www.dorsetforyou.gov.uk/councillors' # DorsetCouncil
                                    assert ward_url == 'https://democracy.bcpcouncil.gov.uk/mgMemberIndex.aspx?bcr=1 # Christchurch
                                    # """
                                print(f"    <{item_name}> scraped (ward members): {ward_member_names}") if PRINT else None
                        else:
                            app_df.at[data_name] = item_value
                            print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                    except KeyError:
                        app_df[data_name] = item_value
                        print(f"    <{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
            # --- --- --- View Documents --- --- ---  # Empty table with one row: There are currently no scanned documents for this application.
            elif 'Documents' in tab_name:
                def get_documents():
                    table_path = '//*[@id="ctl00_ContentPlaceHolder1_pvDocuments"]/table' if scraper_name == 'DorsetCouncil' else '//*[@id="ctl00_ContentPlaceHolder1_DocumentsGrid_ctl00"]'
                    document_table = driver.find_element(By.XPATH, table_path)
                    document_items = document_table.find_elements(By.XPATH, './tbody/tr')
                    if len(document_items[0].find_elements(By.XPATH, './td')) == 1:
                        print(f"\n{tab_index+2}. Document Tab: 0 items")
                        return 0, [], [], []
                    n_items = len(document_items)
                    print(f"\n{tab_index+2}. Document Tab: {n_items} items")

                    n_documents, file_urls, document_names, payloads = 0, [], [], []
                    for document_item in document_items:
                        n_documents += 1
                        file_url = app_df.at['url'] # +'?recno=30944'
                        file_urls.append(file_url)

                        #record_id = file_url.split('recno=')[-1] # 30944
                        print('--- --- --- ', n_documents, ' --- --- ---', file_url)
                        VIEWSTATE = driver.find_element(By.XPATH, '//*[@id="__VIEWSTATE"]').get_attribute('value').strip()
                        #print('VIEWSTATE: ', VIEWSTATE)
                        EVENTTARGET = 'ctl00$ContentPlaceHolder1$DocumentsGrid'  # driver.find_element(By.XPATH, '//*[@id="__EVENTTARGET"]').get_attribute('value').strip()  # 'ctl00$ContentPlaceHolder1$DocumentsGrid'
                        #print('EVENTTARGET: ', EVENTTARGET)
                        EVENTARGUMENT = f'RowClicked:{n_documents-1}'  # driver.find_element(By.XPATH, '//*[@id="__EVENTARGUMENT"]').get_attribute('value')
                        #print('EVENTARGUMENT:', EVENTARGUMENT)
                        VIEWSTATEGENERATOR = driver.find_element(By.XPATH, '//*[@id="__VIEWSTATEGENERATOR"]').get_attribute('value').strip()  # 2DBBAB01
                        #print('VIEWSTATEGENERATOR: ', VIEWSTATEGENERATOR)
                        EVENTVALIDATION = driver.find_element(By.XPATH, '//*[@id="__EVENTVALIDATION"]').get_attribute('value').strip()
                        #print('EVENTVALIDATION: ', EVENTVALIDATION)
                        ctl00_ContentPlaceHolder1_RadTabStrip1_ClientState = driver.find_element(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_RadTabStrip1_ClientState"]').get_attribute('value').strip()
                        #print(ctl00_ContentPlaceHolder1_RadTabStrip1_ClientState)  # {&quot;selectedIndexes&quot;:[&quot;2&quot;],&quot;logEntries&quot;:[],&quot;scrollState&quot;:{}}
                        ctl00_ContentPlaceHolder1_RadMultiPage1_ClientState = driver.find_element(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_RadMultiPage1_ClientState"]').get_attribute('value').strip()
                        #print(ctl00_ContentPlaceHolder1_RadMultiPage1_ClientState)  # {&quot;selectedIndex&quot;:2,&quot;changeLog&quot;:[]}
                        payload = {
                            #'recno': record_id,
                            '__EVENTTARGET': EVENTTARGET,
                            '__EVENTARGUMENT': EVENTARGUMENT,
                            '__VIEWSTATE': VIEWSTATE,
                            '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
                            '__EVENTVALIDATION': EVENTVALIDATION,
                            'ctl00_ContentPlaceHolder1_RadTabStrip1_ClientState': ctl00_ContentPlaceHolder1_RadTabStrip1_ClientState,
                            'ctl00_ContentPlaceHolder1_DocumentsGrid_ClientState': '',
                            'ctl00_ContentPlaceHolder1_RadMultiPage1_ClientState': ctl00_ContentPlaceHolder1_RadMultiPage1_ClientState,
                            'ctl00$ContentPlaceHolder1$tbName': '',
                            'ctl00$ContentPlaceHolder1$tbEmailAddress': '',
                        }
                        payloads.append(payload)

                        document_name_string = document_item.find_element(By.XPATH, './td[2]/a').text.strip()
                        document_name_strings = document_name_string.split('.')
                        print(document_name_strings)

                        # No item_extension, get it from pipeline.py, response.headers
                        document_name_strings = document_name_strings[0].split('-')
                        document_date = document_name_strings[0].strip()
                        document_description = document_name_strings[-1].strip()
                        document_name = f"date={document_date}&desc={document_description}&uid={n_documents}"

                        print(f"    Document {n_documents}: {document_name}") if PRINT else None
                        document_name = replace_invalid_characters(document_name)
                        document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                    app_df.at['other_fields.n_documents'] = n_documents
                    print(f'Total documents: {n_documents}') if PRINT else None
                    return n_documents, file_urls, document_names, payloads
                n_documents, file_urls, document_names, payloads = get_documents()
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names, payloads)
                    yield item
            # --- --- --- Consultees --- --- --- # Similar to Atrium, but the first table locates at a sub-layer.
            elif 'Consultees' in tab_name:
                def parse_consultees():
                    # check tables: neighbour list, consultee list, public notices ...
                    table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_pvConsultees"]')))
                    table_names = table_list.find_element(By.XPATH, './div[1]').find_elements(By.XPATH, './span')
                    table_names.extend(table_list.find_elements(By.XPATH, './span'))

                    table_path = './div/table'
                    tables = table_list.find_element(By.XPATH, './div[1]').find_elements(By.XPATH, table_path)
                    tables.extend(table_list.find_elements(By.XPATH, table_path))

                    n_tables = len(tables)
                    print(f"\n{tab_index+2}. {tab_name} Tab: {n_tables} tables.") if PRINT else None
                    table_name_dict = {'Neighbour List': 'neighbour comments',
                                       'Consultee List': 'consultee comments',
                                       'Public Notices': 'public notices'}
                    csv_names = [table_name_dict[table_name.text.strip()] for table_name in table_names]
                    n_table_items = self.scrape_multi_tables_for_csv(csv_names, tables, folder_name, table_path='tbody/tr', column_path='th', item_path='td')

                    for csv_name, n_items in zip(csv_names, n_table_items):
                        if csv_name == 'neighbour comments':
                            app_df.at['other_fields.n_comments_public_received'] = n_items
                        elif csv_name == 'consultee comments':
                            app_df.at['other_fields.n_comments_consultee_responded'] = n_items
                        elif csv_name == 'public notices':
                            app_df.at['other_fields.n_public_notices'] = n_items
                    app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at['other_fields.n_comments_consultee_responded']
                    print(f"number of comments: {app_df.at['other_fields.n_comments']}")
                parse_consultees()
            # --- --- --- Appeals --- --- ---
            # Validated on both: data items, empty table.
            # Not validated: Non-empty table.
            elif 'Appeals' in tab_name:
                def parse_appeals():
                    appeal_panel = driver.find_element(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_pvAppeals"]')
                    appeal_blocks = appeal_panel.find_elements(By.XPATH, './span/span/div')

                    items, item_values = [], []
                    for appeal_block in appeal_blocks:
                        items.extend(appeal_block.find_elements(By.XPATH, './span'))
                        item_values.extend(appeal_block.find_elements(By.XPATH, './p'))
                    n_items = len(items)
                    print(f"\n{tab_index+2}. {tab_name} Tab: {n_items} data items.")
                    for item, value in zip(items, item_values):
                        item_name = f'Appeal_{item.text.strip()}'
                        data_name = self.details_dict[item_name]
                        item_value = value.text.strip()
                        try:
                            app_df.at[data_name] = item_value
                            print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                        # New
                        except KeyError:
                            app_df[data_name] = item_value
                            print(f"    <{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
                    if n_items > 0:
                        appeal_path = './div/table' if scraper_name == 'DorsetCouncil' else './div/div/table'
                    else: # No appeals.
                        appeal_path = './div/div/table' if scraper_name == 'DorsetCouncil' else './div/div/div/table'
                    appeal_table = appeal_panel.find_element(By.XPATH, appeal_path)
                    table_items = appeal_table.find_elements(By.XPATH, './tbody/tr')
                    table_columns = table_items[0].find_elements(By.XPATH, './th')
                    if len(table_columns) > 0:
                        n_items = len(table_items) - 1  # exclude the column row.
                        print(f"{tab_index+2}. {tab_name} Tab: {n_items} table items.")  # if PRINT else None
                        self.scrape_for_csv(csv_name='appeals', table_columns=table_columns, table_items=table_items[1:], folder_name=folder_name, path='td')
                    else:
                        print(f"{tab_index+2}. " + table_items[0].find_element(By.XPATH, f'./td').text.strip())
                parse_appeals()
            # --- --- --- History --- --- ---
            elif 'History' in tab_name:
                def parse_history():
                    table = driver.find_element(By.XPATH, f'//*[@id="ctl00_ContentPlaceHolder1_gridLinks"]')
                    table_items = table.find_elements(By.XPATH, './tbody/tr')
                    table_columns = table_items[0].find_elements(By.XPATH, './th')
                    if len(table_columns) > 0:
                        n_items = len(table_items) - 1  # exclude the column row.
                        print(f"\n{tab_index+2}. {tab_name} Tab: {n_items} items.") #if PRINT else None
                        self.scrape_for_csv(csv_name='history', table_columns=table_columns, table_items=table_items[1:], folder_name=folder_name, path='td')
                    else:
                        print(f"\n{tab_index+2}. " + table_items[0].find_element(By.XPATH, f'./td').text.strip())
                parse_history()
            else:
                print('Unknown tab: ', tab_name)
                assert 0 == 1
        self.ending(app_df)