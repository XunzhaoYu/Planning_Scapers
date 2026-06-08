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


class Wrexham_Scraper(Base_Scraper):
    name = 'Wrexham_Scraper'
    """
    auth_id = 421(417), Wrexham: was CivicaJason scraper, but is quite different from CivicaJason scrapers now.
        url error.              https://planning.wrexham.gov.uk/planning/planning-application?RefType=GFPlanning&KeyNo=69899
        cymraeg search page:    https://register.wrexham.gov.uk/pr/s/register-view?c__r=Arcus_BE_Public_Register&language=en_GB
                app page:       https://register.wrexham.gov.uk/pr/s/detail/a0lJ7000000TviIIAS?c__r=Arcus_BE_Public_Register&language=en_GB
    """

    # use pipelines_extension to obtain file extensions.
    # custom_settings = {'ITEM_PIPELINES': {'UKPlanning.pipelines.pipelines_extension.DownloadFilesPipeline': 1, }}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All sub_classes of Base_Scraper should define their self.parse_func(s) in __init__
        self.parse_func = self.parse_data_item_Wrexham

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

    def parse_data_item_Wrexham(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta['driver']
        scraper_name = app_df.at['scraper_name']
        folder_name = self.setup_storage_path(app_df)
        max_file_name_len = self.max_folder_file_name_len - len(folder_name) - 5  # 5 chars for suffix/extension, such as .pdf
        print(f'parse_data_item_Wrexham, scraper name: {scraper_name}, max_file_name_len: {max_file_name_len}.')

        try:
            content = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="contentStart"]/div')))
        except TimeoutException:
            # Planning Application details not available.
            note = response.xpath('//*[@id="main-content"]/article/h1/text()').get()
            print('note: ', note)
            return
        header_details = content.find_elements(By.XPATH, '//*[@id="contentStart"]/div/div[1]/arcuscommunity-pr_record-banner/div[2]/div')

        self.ending(app_df)