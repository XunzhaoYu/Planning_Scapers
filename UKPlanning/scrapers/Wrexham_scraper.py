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
    Framework: LWC&Aura / Salesforce Lightning. Features: 1>data-aura-rendered-by, 2> HTML labels with prefix 'c-'
    Note:
    Have temp ids (i.e.: [@id="473:0"]), do not use these temp ids to locate elements in Salesforce.
    For elements 'arcuscommunity-pr...', it could be shadow DOM and could be not accessible via XPATH. Execute script to access.
    
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

        header_details = content.find_elements(By.XPATH, './div[1]/arcuscommunity-pr_record-banner/div[2]/div')
        items = [item.find_element(By.XPATH, './dl/div/dt') for item in header_details]
        item_values = [item.find_element(By.XPATH, './dl/div/dd') for item in header_details]
        app_df = scrape_data_items(app_df, items, item_values, self.details_dict, PRINT)

        # Salesforce Lightning framework: Click tab button to load DOM contents (sections).
        tab_list = content.find_elements(By.XPATH, './/div[@role="tablist"]/ul/li/a') # './div[2]/div/div[@role="tablist"]/ul/li')
        n_tabs = len(tab_list)
        for tab_index, tab in enumerate(tab_list):
            tab_name = tab.get_attribute('innerText').strip()
            tab.click()
            time.sleep(random.uniform(1., 1.5))
            tab_panel = content.find_elements(By.XPATH, './/section[@role="tabpanel"]')[-1] # './div[2]/div/section[@role="tabpanel"]')

            # Details tab:
            if 'details' in tab_name.lower():
                items = tab_panel.find_elements(By.XPATH, './/dt[@class="pr-summary-list__key"]')
                item_values = tab_panel.find_elements(By.XPATH, './/dd[@class="pr-summary-list__value"]')
                print(f'Tab {tab_index + 1}/{n_tabs}: Details. {len(items)} items.')
                app_df = scrape_data_items(app_df, items, item_values, self.details_dict, PRINT)
            # Comments tab:
            elif 'comments' in tab_name.lower():
                # https://register.wrexham.gov.uk/pr/s/detail/a0lJ7000000TuwhIAC?c__r=Arcus_BE_Public_Register&language=en_GB
                # https://register.wrexham.gov.uk/pr/s/detail/a0lJ7000000TuyfIAC?c__r=Arcus_BE_Public_Register&language=en_GB
                # https://register.wrexham.gov.uk/pr/s/detail/a0lJ7000000Tut8IAC?c__r=Arcus_BE_Public_Register&language=en_GB
                print(f'Tab {tab_index + 1}/{n_tabs}: Comments.')
                print(tab_panel.find_element(By.XPATH, './div/div/arcuscommunity-pr_comments/div/div/c-pr_filter/div/div[2]/div[2]/slot/p').get_attribute('innerHTML'))
            # Files tab:
            elif 'files' in tab_name.lower():
                n_documents, n_document_pages, next_button = 0, 1, None
                try:
                    file_panel = tab_panel.find_element(By.XPATH, './div/div/arcuscommunity-pr_files-list/div/c-pr_filter/div/div[2]') # and @class="pr-filter-layout__content"]')
                    # click 'show details' button:
                    buttons = file_panel.find_elements(By.XPATH, './div[1]//button')
                    buttons[1].click()
                    time.sleep(random.uniform(1., 1.5))

                    # get document items:
                    document_items = file_panel.find_elements(By.XPATH, './div[2]//tr[@class="pr-table__row"]')
                    #columns = document_items[0].find_elements(By.XPATH, './th')
                    #print(f'columns: {len(columns)}')
                    n_documents = len(document_items)-1  # the first item is thead.
                    if n_documents == 20:  # have more file pages.
                        try: # n_documents > 20
                            # file_panel. /div[2]/slot/c-pr_pagination/nav/p[2] (pr-pagination__results)/b[3]
                            n_documents = int(file_panel.find_element(By.XPATH, './/p[@class="pr-pagination__results"]/b[3]').get_attribute('innerHTML').strip())

                            # file_panel. /div[2]/slot/c-pr_pagination/nav/ul (pr-pagination__list)/li
                            page_nav_buttons = file_panel.find_elements(By.XPATH, './/ul[@class="pr-pagination__list"]/li')
                            n_document_pages = len(page_nav_buttons)-1
                            next_button = page_nav_buttons[-1]
                        except NoSuchElementException: # n_documents == 20
                            pass
                    print(f'Tab {tab_index + 1}/{n_tabs}: Files. {n_documents} files.')
                except NoSuchElementException:
                    print(f'Tab {tab_index + 1}/{n_tabs}: Files. {n_documents} files.')
                    print(tab_panel.find_element(By.XPATH, './div/div/arcuscommunity-pr_files-list/div/div').get_attribute('innerHTML').strip())
                app_df.at['other_fields.n_documents'] = n_documents
                if n_documents > 0: # get file urls and doc names.
                    file_urls, document_names, doc_no, page_no = [], [], 0, 0
                    while page_no < n_document_pages:
                        page_no += 1
                        for document_item in document_items[1:]:
                            doc_no += 1
                            print(f'    - - - Document {doc_no} - - -') if PRINT else None
                            file_url = document_item.find_element(By.XPATH, './td[5]/a').get_attribute('href')
                            print(f'    {file_url}') if PRINT else None
                            file_urls.append(file_url)

                            document_date = document_item.find_element(By.XPATH, './td[1]').text.strip()
                            document_type = document_item.find_element(By.XPATH, './td[2]').text.strip()
                            document_description = document_item.find_element(By.XPATH, './td[4]').text.strip()
                            document_extension = document_item.find_element(By.XPATH, './td[5]').text.strip().split(',')[0][10:].lower()
                            document_name = f"date={document_date}&type={document_type}&desc={document_description}&uid={doc_no}.{document_extension}"

                            len_limitation = len(document_name) - max_file_name_len
                            print(f'    Doc {doc_no} len_limitation: {len_limitation}') if len_limitation > -5 else None
                            if len_limitation > 0:
                                document_description = document_description[:-len_limitation]
                                document_name = f'date={document_date}&type={document_type}&desc={document_description}&uid={doc_no}.{document_extension}'
                            print(f'    Document {doc_no}: {document_name}') if PRINT else None
                            document_name = replace_invalid_characters(document_name)
                            # print('new: ', document_name) if PRINT else None
                            document_names.append(f'{self.data_upload_path}{folder_name}/{document_name}')
                        if page_no < n_document_pages:
                            next_button.click()
                            time.sleep(random.uniform(1., 1.5))
                            document_items = file_panel.find_elements(By.XPATH, './div[2]//tr[@class="pr-table__row"]')

                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            else:
                print(f'\n{tab_index + 1}. Unknown Tab: {tab_name}.')
                assert 1 == 0
        self.ending(app_df)