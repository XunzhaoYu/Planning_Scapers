import os, time, random
import pandas as pd

from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from configs.settings import PRINT
from general.base_scraper import Base_Scraper
from general.document_utils import replace_invalid_characters, get_documents
from general.items import DownloadFilesItem
from general.utils import unique_columns, scrape_data_items, scrape_for_csv, scrape_multi_tables_for_csv  # test for further re-organization.


class Ocella_Scraper(Base_Scraper):
    name = 'Ocella_Scraper'
    """
    1.auth_id = 12, Arun: https://www1.arun.gov.uk/aplanning/OcellaWeb/planningDetails?reference=EP/2/01/&from=planningSearch
    2.auth_id = 40, Breckland: 
    (not accessible)                 http://planning.breckland.gov.uk/OcellaWeb/planningDetails?from=planningSearch&reference=3PL%2F2021%2F0009%2FHOU
    (Have transferred to a Tascomi?) https://publicportal.breckland.gov.uk/planning/index.html?fa=getApplication&id=136559
    3.auth_id = 141(139), Fareham: https://www.fareham.gov.uk/casetrackerplanning/ApplicationDetails.aspx?reference=P/21/0029/FP&uprn=100060338063
    4.auth_id = 157(155), GreatYarmouth: https://planning.great-yarmouth.gov.uk/OcellaWeb/planningDetails?from=planningSearch&reference=06%2F22%2F0013%2FCD
    5.auth_id = 176(174), Havering: https://development.havering.gov.uk/OcellaWeb/planningDetails?reference=P0010.21&from=planningSearch
    6.auth_id = 182(180), Hillingdon: https://planning.hillingdon.gov.uk/OcellaWeb/planningDetails?from=planningSearch&reference=5783%2FAPP%2F2022%2F10
    7.auth_id = 302(299), Rother: 
    (The address is no longer in use) https://planweb01.rother.gov.uk/OcellaWeb/planningDetails?reference=RR/2022/46/CM&from=planningSearch
    (Have transferred to a Tascomi?)  https://online.rother.gov.uk/planning/index.html?fa=getApplication&id=174816
    8.auth_id = 333(330), SouthHolland: https://planning.sholland.gov.uk/OcellaWeb/planningDetails?from=planningSearch&reference=H14-0012-22
    """

    # use pipelines_extension to obtain file extensions.
    # custom_settings = {'ITEM_PIPELINES': {'UKPlanning.pipelines.pipelines_extension.DownloadFilesPipeline': 1, }}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All sub_classes of Base_Scraper should define their self.parse_func(s) in __init__
        self.parse_func = self.parse_data_item_Ocella

    # details_dict

    def create_item(self, driver, folder_name, file_urls, document_names):
        if not os.path.exists(self.failed_downloads_path + folder_name):
            os.mkdir(self.failed_downloads_path + folder_name)

        item = DownloadFilesItem()
        item['file_urls'] = file_urls
        item['document_names'] = document_names

        cookies = driver.get_cookies()
        print(f'cookies:, {cookies}') if PRINT else None
        item['session_cookies'] = cookies
        return item

    def parse_data_item_Ocella(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta['driver']
        scraper_name = app_df.at['scraper_name']
        folder_name = self.setup_storage_path(app_df)
        max_file_name_len = self.max_folder_file_name_len - len(folder_name) - 5  # 5 chars for suffix/extension, such as .pdf
        print(f'parse_data_item_Ocella, scraper name: {scraper_name}, max_file_name_len: {max_file_name_len}.')

        try:
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="content"]/div/section')))
        except TimeoutException:
            # Planning Application details not available.
            note = response.xpath('//*[@id="main-content"]/article/h1/text()').get()
            print('note: ', note)
            return

        self.ending(app_df)