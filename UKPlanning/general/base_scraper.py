import time, re, os, sys, random, timeit
from datetime import datetime

import pandas as pd
pd.options.mode.chained_assignment = None
import scrapy
from scrapy_selenium import SeleniumRequest
from scrapy import signals

from configs.settings import PRINT, CLOUD_MODE, DEVELOPMENT_MODE
from general.utils import get_list_storage_path, get_data_storage_path, get_filenames, replace_invalid_characters
from tools.curl import upload_file, upload_folder

MAX_FILE_PATH_LEN = 240 # 245

class Base_Scraper(scrapy.Spider):
    name = 'Base_Scraper'

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

        # data_storage_path
        #   ├──── auth1
        #   │       ├───── 2000
        #   │       ├───── 2001
        #   │       ├───── ...
        #   ├──── auth2
        #   │       ├───── 2000
        #   │       ├───── 2001
        #   │       ├───── ...
        #   ├──── auth...
        def initialize_paths_for_auth_year(data_storage_path, auth, year):
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

        # get authority name by auth_index.
        # 根据authority的index, 获取authority的名称
        auth_names = os.listdir(get_list_storage_path())
        auth_names = [auth_name for auth_name in auth_names if not auth_name.startswith('.')]
        auth_names.sort(key=str.lower)
        self.auth = auth_names[int(self.auth_index)]
        self.data_storage_path, self.data_upload_path = initialize_paths_for_auth_year(self.data_storage_path, self.auth, self.year)

        len_data_storage_path = len(self.data_storage_path)
        self.max_folder_file_name_len = MAX_FILE_PATH_LEN - len_data_storage_path
        # print(len_data_storage_path, self.max_folder_file_name_len)

        if DEVELOPMENT_MODE:
            # sample one application of self.auth per year in the given range of years.
            # 在给定years的范围内对authority进行每年一个application的采样
            test_index, test_year_from, test_year_end = 4, 11, 12  # Variables for test / development.
            app_dfs = []
            filenames = get_filenames(f"{get_list_storage_path()}{self.auth}/")
            print(f"{self.auth}. number of files: {len(filenames)}")
            for filename in filenames[test_year_from:test_year_end]:
                file_path = f"{get_list_storage_path()}{self.auth}/{filename}"
                df = pd.read_csv(file_path)
                print(filename, df.shape[0])
                app_df = df.iloc[test_index, :]
                app_dfs.append(app_df)
            self.app_dfs = pd.concat([pd.DataFrame(app_df).T for app_df in app_dfs], ignore_index=True)
        else:
            # get all applications of self.auth in the given year.
            # 获取authority在给定year的所有applications
            # option1: scrape all years
            year = int(self.year)
            if year < 0:
                src_path = f"{get_list_storage_path()}{self.auth}/"
                src_filenames = os.listdir(src_path)
                src_filenames.sort(key=str.lower)
                src_files = [src_path + filename for filename in src_filenames if not filename.startswith('.')]
                self.app_dfs = pd.concat([pd.read_csv(file) for file in src_files], ignore_index=True)
            # option2: scrape a given year
            else:
                src_path = f"{get_list_storage_path()}{self.auth}/{self.auth}{self.year}.csv"
                self.app_dfs = pd.read_csv(src_path)

            # load the list of to_scrape.
            # 加载to_scrape_list文件, 获取尚未爬取的applications的list
            self.list_path = f"{self.data_storage_path}to_scrape_list.csv"
            if not os.path.isfile(self.list_path): # scrape from the scratch.
                self.init_index = 0
                self.to_scrape = self.app_dfs.iloc[self.init_index:, 0]
                self.to_scrape.to_csv(self.list_path, index=True)
                print("save", self.to_scrape)
            else: # continue from the exit point of the last run.
                self.to_scrape = pd.read_csv(self.list_path, index_col=0)
                if self.to_scrape.empty:
                    sys.exit("To_scrape_list is empty.")
                else:
                    print("load", self.to_scrape)
            self.app_dfs = self.app_dfs.iloc[self.to_scrape.index, :]
        print(self.app_dfs)

        # settings.
        self.index = -1
        self.failures = 0

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

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(Base_Scraper, cls).from_crawler(crawler, *args, **kwargs)
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
        # summarize scraped application data into a single csv file:
        # 将爬取的application data合并到一个csv文件中:
        filenames = os.listdir(self.result_storage_path)
        filenames.sort(key=str.lower)
        files = [self.result_storage_path + filename for filename in filenames if not filename.startswith('.')]
        append_df = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)

        current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        if DEVELOPMENT_MODE:
            append_df.to_csv(self.data_storage_path + f'result_{current_time}.csv', index=False)
            # upload_file(f'result_{current_time}.csv') if CLOUD_MODE else None
        else:
            # delete previous csv files before saving the new one.
            # 把之前的总结文件删除, 再存储新的总结文件
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
            yield SeleniumRequest(url=url, callback=self.parse_func, meta={'app_df': app_df})  # para: dont_filter=True
            # yield SeleniumRequest(url=url, callback=self.parse_func, meta={'app_df': app_df, 'valid_IPs': self.init_valid_IPs})
        except IndexError:
            print("list is empty.")
            return

    """
    Auxiliary Functions within the Class Base_Scraper
    """
    # setup storage path for the current application.
    def setup_storage_path(self, app_df):
        folder_name = str(app_df.at['name'])
        folder_name = replace_invalid_characters(folder_name)
        folder_path = f"{self.data_storage_path}{folder_name}/"
        print(folder_path) if PRINT else None
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
        upload_folder(f"{self.data_upload_path}{folder_name}/") if CLOUD_MODE else None
        return folder_name

    # set default items for the current application.
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

    # the end of scraping an application.
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