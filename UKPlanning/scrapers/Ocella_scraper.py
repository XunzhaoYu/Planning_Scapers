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
    4.auth_id = 157(155), GreatYarmouth: https://planning.great-yarmouth.gov.uk/OcellaWeb/planningDetails?from=planningSearch&reference=06%2F22%2F0013%2FCD
    5.auth_id = 176(174), Havering: https://development.havering.gov.uk/OcellaWeb/planningDetails?reference=P0010.21&from=planningSearch
    6.auth_id = 182(180), Hillingdon: https://planning.hillingdon.gov.uk/OcellaWeb/planningDetails?from=planningSearch&reference=5783%2FAPP%2F2022%2F10
    8.auth_id = 333(330), SouthHolland: https://planning.sholland.gov.uk/OcellaWeb/planningDetails?from=planningSearch&reference=H14-0012-22
    """
    # not Ocella anymore.
    # 3.auth_id = 141(139), Fareham: https://www.fareham.gov.uk/casetrackerplanning/ApplicationDetails.aspx?reference=P/21/0029/FP&uprn=100060338063
    # was Ocella, but is Tascomi now:
    """ 
    2.auth_id = 40, Breckland: 
    (not accessible)                 http://planning.breckland.gov.uk/OcellaWeb/planningDetails?from=planningSearch&reference=3PL%2F2021%2F0009%2FHOU
    (Have transferred to a Tascomi?) https://publicportal.breckland.gov.uk/planning/index.html?fa=getApplication&id=136559
    7.auth_id = 302(299), Rother: 
    (The address is no longer in use) https://planweb01.rother.gov.uk/OcellaWeb/planningDetails?reference=RR/2022/46/CM&from=planningSearch
    (Have transferred to a Tascomi?)  https://online.rother.gov.uk/planning/index.html?fa=getApplication&id=174816 
    """

    # use pipelines_extension to obtain file extensions.
    custom_settings = {'ITEM_PIPELINES': {'UKPlanning.pipelines.pipelines_extension.DownloadFilesPipeline': 1, }}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All sub_classes of Base_Scraper should define their self.parse_func(s) in __init__
        self.parse_func = self.parse_data_item_Ocella

    details_dict = {'Reference': 'uid',
                    'Status': 'other_fields.status',
                    'Proposal': 'description',
                    'Location': 'address',
                    'Ward': 'other_fields.ward_name',  # Havering
                    'Parish': 'other_fields.parish',  # Arun, GreatYarmouth
                    'Case Officer': 'other_fields.case_officer',
                    'Received': 'other_fields.date_received',
                    'Validated': 'other_fields.date_validated',
                    'Decision By': 'other_fields.target_decision_date',
                    'Comment By': 'other_fields.comment_expires_date',  # Arun
                    'Target Cmte': 'other_fields.target_cmte', # Arun
                    'Neighbours': 'other_fields.comment_expires_date',  # GreatYarmouth
                    'Site Notice': 'other_fields.site_notice_start_date', # Hillingdon
                    'Advertised': 'other_fields.last_advertised_date', # Hillingdon
                    'Decided': 'other_fields.decision_issued_date',
                    'Applicant': 'other_fields.applicant_name',
                    'Agent': 'other_fields.agent_name',
                    'Appeal': 'other_fields.appeal_reference', # Hillingdon
                    'Lodged': 'other_fields.appeal_lodged_date', # Hillingdon
                    'Type': 'other_fields.application_type', # Hillingdon
                    'Decision': 'other_fields.decision' # Hillingdon
                    }

    tab_dict = {'Arun': '/html/body/table[1]/tbody/tr',
                'GreatYarmouth': '/html/body/div/table[1]/tbody/tr',
                'Havering': '//*[@id="content"]/div/div/div[1]/table/tbody/tr',
                'Hillingdon': '//*[@id="LBH_SandwichSource"]/table[1]/tbody/tr',
                'SouthHolland': '//*[@id="main"]/table[1]/tbody/tr' }

    data_dict = {'Arun': '/html/body/table[2]/tbody/tr',
                'GreatYarmouth': '/html/body/div/table[2]/tbody/tr',
                'Havering': '//*[@id="content"]/div/div/div[2]/table/tbody/tr',
                'Hillingdon': '//*[@id="LBH_SandwichSource"]/table[2]/tbody/tr',
                'SouthHolland': '//*[@id="main"]/table[2]/tbody/tr' }

    doc_url_dict = {'Arun': '',
                    'GreatYarmouth': 'https://portal.great-yarmouth.gov.uk/civica/Resource/Civica/Handler.ashx/Doc/pagestream?cd=inline&pdf=true&docno=',
                    'Havering': 'https://msp.havering.gov.uk/civica/Resource/Civica/Handler.ashx/Doc/pagestream?cd=inline&pdf=true&docno=',
                    'Hillingdon': '',
                    'SouthHolland': '' }

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
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, self.tab_dict[scraper_name])))
        except TimeoutException:
            # Planning Application details not available.
            note = response.xpath('//*[@id="main-content"]/article/h1/text()').get()
            print('note: ', note)
            return

        detail_list = driver.find_elements(By.XPATH, self.data_dict[scraper_name])
        items = [detail.find_element(By.XPATH, './td[1]/strong') for detail in detail_list]
        item_values = [detail.find_element(By.XPATH, './td[2]') for detail in detail_list]
        n_items = len(items)
        print(f'\n1. Details Tab: {n_items} items.')
        app_df = scrape_data_items(app_df, items, item_values, self.details_dict, PRINT)

        tab_list = driver.find_elements(By.XPATH, f'{self.tab_dict[scraper_name]}/td')
        n_tabs = len(tab_list)
        for tab_index, tab in enumerate(tab_list):
            tab_name = tab.find_element(By.XPATH, './/input').get_attribute('value').strip()
            # --- --- --- View Documents (doc) --- --- ---
            if 'document' in tab_name.lower():
                #tab.click()
                tab.find_element(By.XPATH, './/input').click()
                time.sleep(2)

                def get_documents_from_table(n_documents, file_urls, document_names, document_items):
                    if 'no documents' in document_items[0].find_element(By.XPATH, './td[1]').text:
                        return n_documents, file_urls, document_names  # There are no documents for this section
                    else:
                        for document_item in document_items:
                            n_documents += 1
                            print(f'        - - - Document {n_documents} - - -') if PRINT else None
                            file_url = document_item.find_element(By.XPATH, './td[1]/a').get_attribute('href')
                            print(f'        {file_url}') if PRINT else None
                            file_urls.append(file_url)
                            document_type = document_item.find_element(By.XPATH, './td[1]').text.strip()
                            document_description = document_item.find_element(By.XPATH, './td[5]').text.strip()
                            document_date = document_item.find_element(By.XPATH, './td[3]').text.strip()
                            #document_extension = file_url.split('.')[-1].split('&')[0]
                            document_name = f'date={document_date}&type={document_type}&desc={document_description}&uid={n_documents}'#.{document_extension}'
                            len_limitation = len(document_name) - max_file_name_len
                            print(f'        Doc {n_documents} len_limitation: {len_limitation}') if len_limitation > -5 else None
                            if len_limitation > 0:
                                document_description = document_description[:-len_limitation]
                                document_name = f'date={document_date}&type={document_type}&desc={document_description}&uid={n_documents}'#.{document_extension}'
                            print(f'        Document {n_documents}: {document_name}') if PRINT else None

                            document_name = replace_invalid_characters(document_name)
                            # print('new: ', document_name) if PRINT else None
                            document_names.append(f'{self.data_upload_path}{folder_name}/{document_name}')
                        return n_documents, file_urls, document_names
                # Single table
                def get_documents_Arun():
                    n_documents, file_urls, document_names = 0, [], []
                    print(f'\n2. Documents Tab.')
                    document_items = driver.find_elements(By.XPATH, '/html/body/table[2]/tbody/tr')
                    n_documents, file_urls, document_names = get_documents_from_table(0, file_urls, document_names, document_items)
                    app_df.at['other_fields.n_documents'] = n_documents
                    return file_urls, document_names
                # Several tables: Hillingdon, SouthHolland
                def get_documents_Hillingdon():
                    n_documents, file_urls, document_names = 0, [], []
                    document_panel = driver.find_element(By.XPATH, '//*[@id="LBH_SandwichSource"] | //*[@id="main"]')
                    table_names = document_panel.find_elements(By.XPATH, './strong')
                    n_tables = len(table_names)
                    print(f'\n2. Documents Tab: {n_tables} tables.')
                    for table_index in range(n_tables):
                        table_name = table_names[table_index].text.strip()
                        print(f'    - - - Document Table {table_index + 1}: {table_name} - - -') if PRINT else None
                        document_items = document_panel.find_elements(By.XPATH,f'./table[{table_index + 1}]/tbody/tr')
                        n_documents, file_urls, document_names = get_documents_from_table(n_documents, file_urls, document_names, document_items)
                    app_df.at['other_fields.n_documents'] = n_documents
                    return file_urls, document_names

                # Civica
                def get_documents_Havering():
                    app_tab = driver.current_window_handle
                    all_tabs = driver.window_handles
                    docs_tab = [x for x in all_tabs if x != app_tab][0]
                    driver.switch_to.window(docs_tab)  # move to doc tab.

                    file_urls, document_names = [], []
                    try:
                        document_list = WebDriverWait(driver, 3).until(EC.visibility_of_element_located((By.CLASS_NAME, 'civica-doclist')))
                        #document_list = driver.find_element(By.CLASS_NAME, 'civica-doclist')
                        document_items = document_list.find_elements(By.XPATH, './ul/li')
                        n_documents = len(document_items)
                    except NoSuchElementException:
                        n_documents = 0
                    app_df.at['other_fields.n_documents'] = n_documents
                    print(f'\n2. Documents Tab: {n_documents} items.')
                    if n_documents > 0:
                        n_documents = 0
                        for document_item in document_items:
                            n_documents += 1
                            print(f'    - - - Document {n_documents} - - -') if PRINT else None
                            # file_url = document_item.find_element(By.XPATH, './a').get_attribute('href')
                            #print(f'    {response.urljoin(file_url)}') if PRINT else None
                            #file_urls.append(response.urljoin(file_url))

                            # https://portal.great-yarmouth.gov.uk/civica/Resource/Civica/Handler.ashx/Doc/pagestream?cd=inline&pdf=true&docno=5364454
                            # https://msp.havering.gov.uk/civica/Resource/Civica/Handler.ashx/Doc/pagestream?cd=inline&pdf=true&docno=2571635
                            file_id = document_item.find_element(By.XPATH, './a').get_attribute('href').split('?DocNo=')[1]
                            file_url = f'{self.doc_url_dict[self.auth]}{file_id}'
                            print(f'    {file_url}') if PRINT else None
                            file_urls.append(file_url)

                            document_date = document_item.find_element(By.CLASS_NAME, 'civica-doclistdetailtext').text.strip()
                            document_description = document_item.find_element(By.CLASS_NAME, 'civica-doclisttitletext').text.strip()
                            document_name = f"date={document_date}&desc={document_description}&uid={n_documents}"#.{item_extension}"

                            len_limitation = len(document_name) - max_file_name_len
                            print(f'    Doc {n_documents} len_limitation: {len_limitation}') if len_limitation > -5 else None
                            if len_limitation > 0:
                                document_description = document_description[:-len_limitation]
                                document_name = f'date={document_date}&desc={document_description}&uid={n_documents}'
                            print(f'    Document {n_documents}: {document_name}') if PRINT else None
                            document_name = replace_invalid_characters(document_name)
                            # print('new: ', document_name) if PRINT else None
                            document_names.append(f'{self.data_upload_path}{folder_name}/{document_name}')

                    driver.close()
                    driver.switch_to.window(app_tab)
                    return file_urls, document_names

                if self.auth == 'Arun':
                    file_urls, document_names = get_documents_Arun()
                elif self.auth in ['Hillingdon', 'SouthHolland']:
                    file_urls, document_names = get_documents_Hillingdon()
                else:
                    file_urls, document_names = get_documents_Havering()
                if len(file_urls) > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
                break
            else:
                print(f'\n{tab_index + 1}. Unknown Tab: {tab_name}.')
                assert 1 == 0

        self.ending(app_df)