import scrapy
from scrapy import signals
from items import DownloadFilesItem
from settings import PRINT, CLOUD_MODE
from scrapy_selenium import SeleniumRequest
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
from tools.utils import get_project_root, get_list_storage_path, get_data_storage_path
from tools.curl import upload_file, upload_folder
import os, re, time
from datetime import datetime
import shutil
from spiders.UKPlanning_Scraper import UKPlanning_Scraper


def combine_csv():
    auth_names = os.listdir(get_list_storage_path())
    auth_names = [auth_name for auth_name in auth_names if not auth_name.startswith('.')]
    auth_names.sort(key=str.lower)

    auth = auth_names[1]
    result_storage_path = f"{get_data_storage_path()}0.results/"
    #list_path = f"{get_data_storage_path()}to_scrape_list.csv"
    if not os.path.exists(result_storage_path):
        os.mkdir(result_storage_path)
    filenames = os.listdir(result_storage_path)
    filenames.sort(key=str.lower)
    files = [result_storage_path + filename for filename in filenames if not filename.startswith('.')]
    append_df = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
    current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    append_df.to_csv(get_data_storage_path() + f'{auth}_result_{current_time}.csv', index=False)
#combine_csv()

# Need use scraped_list to find the application link to re-download.
def load_scraped_list(src_path):
    filenames = os.listdir(src_path)
    pattern = r'\w*_result_\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}.csv'
    list_file = None
    try:
        for filename in filenames:
            match = re.search(pattern, filename, re.I)
            if match:
                list_file = src_path + match.group()
                break
        scraped_list = pd.read_csv(list_file, index_col=0)
    except:
        scraped_list = None
    return scraped_list

# USELESS now.
"""
# USELESS now. New re-download scraper takes auth_name as an argument.
def get_authority_name(src_path):  # Bassetlaw
    filenames = os.listdir(src_path)
    pattern = r'\w*_result_\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}.csv'
    try:
        for filename in filenames:
            match = re.search(pattern, filename, re.I)
            if match:
                return match.group().split('_')[0]
        return None
    except:
        return None

# USELESS now. An auxiliary function for update_to_scrape_list() and check_redownloaded_apps().
def load_to_scrape():
    # read the list of scraping.
    list_path = f"{get_data_storage_path()}to_scrape_list.csv"
    to_scrape = pd.read_csv(list_path, index_col=0)
    #print("read", to_scrape)
    return to_scrape

# USELESS now. An auxiliary function for update_to_scrape_list().
def save_to_scrape(to_scrape):
    list_path = f"{get_data_storage_path()}to_scrape_list.csv"
    to_scrape.to_csv(list_path, index=True)

# If to_scrape_list has not been updated, scan the 0.results folder and then update the list with the completed applications.
# USELESS now. The UKPlanning_Scraper was revised to update to_scrape_list iteratively.
def update_to_scrape_list(auth, year):
    # result_storage_path = f"{get_data_storage_path()}0.results/"
    result_storage_path = f"{get_data_storage_path()}{auth}/{year}/0.results/"
    if not os.path.exists(result_storage_path):
        os.mkdir(result_storage_path)
    filenames = os.listdir(result_storage_path)
    filenames.sort(key=str.lower)
    files = [int(filename[:4]) for filename in filenames if not filename.startswith('.')]

    to_scrape = load_to_scrape()
    for i, file in enumerate(files):
        print(file, type(file))
        to_scrape.drop(file, inplace=True)
    save_to_scrape(to_scrape)
#update_to_scrape_list()

# USELESS now. All apps that need re-download have been saved in 'failed_downloads' now.
def check_redownloaded_apps():
    src_path = get_data_storage_path()
    redownload_apps = os.listdir(src_path)
    to_scrape = load_to_scrape()
    authority_name = get_authority_name(src_path)
    print(authority_name)

    re_download_list = []
    for app in redownload_apps:
        if app.startswith(f'{authority_name}-'):
            try:
                app_id = re.sub('-', '/', app)
                app_df = to_scrape.loc[to_scrape['name'] == app_id]
                app_index = app_df.iloc[0].name
                #print(app_index, app_id)
            except:
                re_download_list.append(app)
                #print('miss', app)
    return re_download_list
#check_redownloaded_apps()

# USELESS now. All apps that need re-download have been saved in 'failed_downloads' now.
def save_document_unavailable_list(redownload_apps):
    unavailable_list = []
    src_path = get_data_storage_path()
    for redownload_app in redownload_apps:
        if os.path.exists(src_path + redownload_app):
            unavailable_list.append(redownload_app)
    if len(unavailable_list) == 0:
        return
    else:
        #print("unavailable list: ", unavailable_list)
        df = pd.DataFrame(unavailable_list, columns=['name'])
    # check if already has a list. If yes, append to the existing list; otherwise, create a new list.
    list_path = f"{src_path}document_unavailable_list.csv"
    if os.path.isfile(list_path):
        existing_df = pd.read_csv(list_path, index_col=0)
        df = pd.concat([existing_df, df], ignore_index=True)
    df.to_csv(list_path, index=True)

    for unavailable_app in unavailable_list:
        shutil.rmtree(src_path + unavailable_app)
#redownload_apps = check_redownloaded_apps()
#print('redownload: ', redownload_apps)
#save_document_unavailable_list(redownload_apps)
"""


class UKPlanning_Redownload(UKPlanning_Scraper):
    name = 'UKPlanning_Redownload'

    def __init__(self, auth, year):
        self.start_time = time.time()
        self.data_storage_path = f"{get_data_storage_path()}{auth}/{year}/"
        self.data_upload_path = f"{auth}/{year}/"
        self.failed_downloads_path = f"{self.data_storage_path}failed_downloads/"
        scraped_list = load_scraped_list(self.data_storage_path)
        print(scraped_list)

        self.redownload_folders = [filename for filename in os.listdir(self.failed_downloads_path) if filename.startswith(auth)]
        self.app_dfs = []
        for folder in self.redownload_folders:
            print(folder)
            app_id = re.sub('-', '/', folder)
            self.app_dfs.append(scraped_list.loc[app_id])
        self.total = len(self.redownload_folders)
        self.index = -1

    def spider_closed(self, spider):
        time_cost = time.time() - self.start_time
        print("final time_cost: {:.0f} mins {:.4f} secs.".format(time_cost // 60, time_cost % 60))

    def start_requests(self):
        self.index += 1
        if self.index < self.total:
            app_df = self.app_dfs[self.index]
            url = app_df.at['other_fields.docs_url']
            print(url)
            yield SeleniumRequest(url=url, callback=self.parse_documents_item, meta={'app_df': app_df, 'folder_name': self.redownload_folders[self.index]})

    def parse_uprn_item(self, response):
        pass

    def parse_map_item(self, response):
        pass

"""
class UKPlanning_Scraper(scrapy.Spider):
    name = 'UKPlanning_Redownload'

    def __init__(self, auth, year):
        super().__init__()

        self.data_storage_path = f"{get_data_storage_path()}{auth}/{year}/"
        scraped_list = load_scraped_list(self.data_storage_path)
        print(scraped_list)

        self.redownload_folders = os.listdir(f"{get_data_storage_path()}fail_downloads/")
        self.app_dfs = []
        for folder in self.redownload_folders:
            print(folder)
            app_id = re.sub('-', '/', folder)
            self.app_dfs.append(scraped_list.loc[app_id])
        self.total = len(self.redownload_folders)
        self.index = -1

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
            self.crawler.engine.crawl(req)

    def spider_closed(self, spider):
        if CLOUD_MODE:
            #save_document_unavailable_list(self.redownload_folders)
            pass

    def start_requests(self):
        self.index += 1
        if self.index < self.total:
            app_df = self.app_dfs[self.index]
            url = app_df.at['other_fields.docs_url']
            print(url)
            yield SeleniumRequest(url=url, callback=self.parse_documents_item, meta={'app_df': app_df, 'folder_name': self.redownload_folders[self.index]})

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

    def rename_documents_and_get_file_urls(self, response, folder_name):
        date_column, type_column, description_column = self.get_document_info_columns(response)
        document_items = response.xpath('//*[@id="Documents"]/tbody/tr')[1:]
        document_paths = []
        file_urls = []
        for i, document_item in enumerate(document_items):
            document_date = document_item.xpath(f'./td[{date_column}]/text()').get().strip()
            document_type = document_item.xpath(f'./td[{type_column}]/text()').get().strip()
            document_description = document_item.xpath(f'./td[{description_column}]/text()').get().strip()
            file_url = document_item.xpath('./td/a')[-1].xpath('./@href').get()
            # file_url = document_item.css('a::attr(href)').get()

            item_identity = file_url.split('-')[-1]
            document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
            #print(document_name) if PRINT else None
            print(folder_name + document_name) if PRINT else None
            if '/' in document_name:
                document_name = re.sub('/', '-', document_name)
            if ' ' in document_name:
                document_name = re.sub(' ', '_', document_name)
            #document_paths.append(folder_name + document_name)
            document_paths.append(f"{self.data_upload_path}{folder_name}/{document_name}")
            file_urls.append(response.urljoin(file_url))
        return document_paths, file_urls

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
                print(f"\n{app_df.name} <doc mode> n_documents: {n_documents}. (None)")
            else:
                n_documents = int(re.search(r"\d+", documents_str).group())
                #print(f"<doc mode> n_documents: {n_documents}")
                print(f"\n{app_df.name} <doc mode> n_documents: {n_documents}, folder_name: {folder_name}")
            # other_fields.n_documents
            app_df.at['other_fields.n_documents'] = n_documents
            if n_documents > 0:
                driver = response.request.meta["driver"]
                document_names, file_urls = self.rename_documents_and_get_file_urls(response, folder_name)
                item = DownloadFilesItem()
                item['file_urls'] = file_urls
                item['document_names'] = document_names

                cookies = driver.get_cookies()
                #print("cookies:", cookies) if PRINT else None
                item['session_cookies'] = cookies
                yield item
        elif mode == 'externalDocuments':
            # self.scrape_external_documents(response, app_df, storage_path)
            docs_url = response.xpath('//*[@id="pa"]/div[3]/div[3]/div[3]/p/a/@href').get()
            app_df.at['other_fields.docs_url'] = docs_url
            print(f'<{mode}> external document link:', docs_url)
            yield SeleniumRequest(url=docs_url, callback=self.parse_documents_item, meta={'app_df': app_df, 'storage_path': storage_path})
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
                    pass
                    #self.scrape_documents_by_NEC(response, n_documents, storage_path)
            if system_name == 'Civica':  # Ryedale
                pass

        else:
            print('Unknown document mode.')
"""

def upload():
    src_path = get_data_storage_path()
    scraped_list = load_scraped_list(src_path)
    print(scraped_list)

    redownload_apps = check_redownloaded_apps()
    for folder_name in redownload_apps:
        try:
            app_id = re.sub('-', '/', folder_name)
            n_documents = scraped_list.loc[app_id].at['other_fields.n_documents']
            print(folder_name, n_documents)
            folder_path = src_path + folder_name
            upload_folder(folder_name + '/')
            filenames = os.listdir(folder_path)
            if len(filenames) == n_documents:
                for filename in filenames:
                    file_path = folder_name + '/' + filename
                    print(src_path + file_path)
                    if upload_file(file_path) == 0:  # upload succeeded, delete local file.
                        os.remove(src_path + file_path)
                if not os.listdir(folder_path):
                    os.rmdir(folder_path)
            else:
                print(f"refuse to upload, please check if all {n_documents} documents have already downloaded: {folder_name}")
        except KeyError:
            pass
#upload()