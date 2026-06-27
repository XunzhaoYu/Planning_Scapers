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
        self.url_check = True
        self.url_preprocess = self.url_preprocess_Wrexham
        self.parse_func = self.parse_data_item_Wrexham

    details_dict = {'Description': 'description',
                    'Site Address': 'address',
                    # Details
                    'Application type': 'other_fields.application_type',
                    'Status': 'other_fields.status',
                    'Officer': 'other_fields.case_officer',
                    # Decision
                    'Decision': 'other_fields.decision',
                    'Decision date': 'other_fields.decision_issued_date',
                    'Decision notice sent date': 'other_fields.decision_notice_date',
                    'Determination level': 'other_fields.expected_decision_level',
                    # Application Life Cycle
                    'Received date': 'other_fields.date_received',
                    'Valid date': 'other_fields.date_validated',
                    'Date of committee': 'other_fields.meeting_date',
                    'Application expiry date': 'other_fields.application_expires_date',
                    # Communities and Wards
                    'Communities': 'other_fields.community_council',
                    'Wards': 'other_fields.ward_name',
                    }

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

    def url_preprocess_Wrexham(self, url):
        if url.startswith('https://register.wrexham.gov.uk/pr'):
            self.parse_func = self.parse_data_item_Wrexham
            return url
        else:
            self.parse_func = self.search_by_appID_Wrexham
            return 'https://register.wrexham.gov.uk/pr/s/register-view?c__r=Arcus_BE_Public_Register&language=en_GB'

    def search_by_appID_Wrexham(self, response):
        driver = response.request.meta['driver']
        app_df = response.meta['app_df']
        url = response.request.url
        print(f'search page url: {url}') if PRINT else None

        # use app_id to search and view the application page.
        app_id = app_df.at['uid']
        input_reference = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//input[@class="slds-input"]')))
        input_reference.click()
        input_reference.send_keys(app_id)
        # click 'search' button.
        time.sleep(random.uniform(1., 1.5))
        #driver.find_element(By.XPATH, '//button[@title="Search"]').click()
        WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '//button[@title="Search"]'))).click()
        time.sleep(random.uniform(1., 1.5))
        driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')  # scroll down to the bottom of this page.
        # search result:
        # //*[@id="contentStart"]/div/div/arcuscommunity-pr_search/div/section[3]/div/div/c-pr_result[3]/div/div[3]/div  # find a result.
        # //*[@id="contentStart"]/div/div/arcuscommunity-pr_search/div/section[3]/div/div/c-pr_result[3]/div/div[3]/p  # No result.
        planning_application_result_block = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='contentStart']/div/div/arcuscommunity-pr_search/div/section[3]/div/div/c-pr_result[3]/div/div[3]")))
        try:
            #search_result = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='contentStart']/div/div/arcuscommunity-pr_search/div/section[3]/div/div/c-pr_result[3]/div/div[3]/div/c-pr_articles/div/div[1]/div[1]/c-pr_formatted-output/div/div/lightning-formatted-url/a")))
            #search_result.click()
            search_result = planning_application_result_block.find_element(By.XPATH, "./div/c-pr_articles/div/div[1]/div[1]/c-pr_formatted-output/div/div/lightning-formatted-url/a")
            driver.execute_script("arguments[0].click();", search_result)
        except TimeoutException:
            print(planning_application_result_block.find_element(By.XPATH, './p').get_attribute('innerHTML'))
            return

        time.sleep(random.uniform(4., 5.))
        app_df.at['url'] = driver.current_url
        print(f'correct url: {driver.current_url}')
        # scrape application
        yield from self.parse_data_item_Wrexham(response)


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
        #header_details = content.find_elements(By.XPATH, '//*[@id="contentStart"]/div/div[1]/arcuscommunity-pr_record-banner/div[2]/div')
        header_details = content.find_elements(By.XPATH, './div[1]/arcuscommunity-pr_record-banner/div[2]/div')
        items = [item.find_element(By.XPATH, './dl/div/dt') for item in header_details]
        item_values = [item.find_element(By.XPATH, './dl/div/dd') for item in header_details]
        app_df = scrape_data_items(app_df, items, item_values, self.details_dict, PRINT)
        #print(items[0].get_attribute('innerHTML'))
        #print(item_values[0].get_attribute('innerHTML'))
        tab_panels = content.find_element(By.XPATH, './div[2]/div')

        self.ending(app_df)