import os, time, random, re
import pandas as pd

from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains

from configs.settings import PRINT
from general.base_scraper import Base_Scraper
from general.document_utils import replace_invalid_characters, get_documents, get_NEC_or_Northgate_documents
from general.items import DownloadFilesItem
from general.utils import unique_columns, scrape_data_items, scrape_for_csv, scrape_multi_tables_for_csv  # test for further re-organization.


class Agile_Scraper(Base_Scraper):
    name = 'Agile_Scraper'
    """
    1.-auth_id = 61, CannockChase: https://planning.agileapplications.co.uk/cannock/application-details/7007
    2.-auth_id = 139(137), Exmoor: https://planning.agileapplications.co.uk/exmoor/application-details/2552
    3.-auth_id = 145(143), Flintshire: https://planning.agileapplications.co.uk/flintshire/application-details/28244
    4.-auth_id = 202(200), LakeDistrict: https://planning.agileapplications.co.uk/ldnpa/application-details/27229
    5.-auth_id = 229(227), Middlesbrough: https://planning.agileapplications.co.uk/middlesbrough/application-details/1781
    6.-auth_id = 236(234), MoleValley: x
    7.-auth_id = 244(242), NewForestPark: https://planning.agileapplications.co.uk/nfnpa/application-details/44335
    8.-auth_id = 275(272), OldOakParkRoyal: https://planning.agileapplications.co.uk/opdc/application-details/8807
    9.auth_id = 281(278), Pembrokeshire: https://planning.agileapplications.co.uk/pembrokeshire/application-details/24026
    10.-auth_id = 291(288), Redbridge: https://planning.agileapplications.co.uk/redbridge/application-details/110591
    11.-auth_id = 304(301), Rugby: https://planning.agileapplications.co.uk/rugby/application-details/2777
    12.-auth_id = 322(319), Slough: https://planning.agileapplications.co.uk/slough/application-details/11430
    13.-auth_id = 346(343), Staffordshire: https://planning.agileapplications.co.uk/staffordshire/application-details/25518
    14.-auth_id = 377(373), Tonbridge: https://planning.agileapplications.co.uk/tmbc/application-details/153514
    15.-auth_id = 427(423), YorkshireDales: https://planning.agileapplications.co.uk/yorkshiredale/application-details/488
    """

    # use pipelines_extension to obtain file extensions.
    # custom_settings = {'ITEM_PIPELINES': {'UKPlanning.pipelines.pipelines_extension.DownloadFilesPipeline': 1, }}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All sub_classes of Base_Scraper should define their self.parse_func(s) in __init__
        self.parse_func = self.parse_data_item_Agile

    details_dict ={'Application reference number': 'uid',
                   'LA reference': 'other_fields.LA_reference', # Flintshire
                   'Application type': 'other_fields.application_type',
                   'Proposal description': 'description',
                   'Location': 'address',
                   'Town or communty council': 'other_fields.parish', # Pembrokeshire
                   'Ward': 'other_fields.ward_name', # Flintshire, Pembrokeshire
                   'Parish': 'other_fields.parish',  # NewForestPark
                   'Area': 'other_fields.parish', # Flintshire
                   'Status': 'other_fields.status',
                   'Status description': 'other_fields.status_description', #

                   'Registration date': 'other_fields.date_validated', # CannockChase
                   'Registered date': 'other_fields.date_validated',  # CannockChase
                   'Validated date': 'other_fields.date_validated', # Pembrokeshire
                   'Extension of time date': 'other_fields.extension_of_time_date', # Pembrokeshire
                   'Decision level': 'other_fields.expected_decision_level',
                   'Decision': 'other_fields.decision',
                   'Decision date': 'other_fields.decision_issued_date',
                   'Decision expiry date': 'other_fields.decision_expiry_date', #

                   'Appeal type': 'other_fields.appeal_type', #
                   'Appeal lodged date': 'other_fields.appeal_lodged_date', # Flintshire, Pembrokeshire
                   'Appeal decision': 'other_fields.appeal_result',
                   'Appeal decision date': 'other_fields.appeal_decision_date',

                   'Agent name/Company name': 'other_fields.agent_name', # Pembrokeshire
                   'Agent name (company)': 'other_fields.agent_name',  # CannockChase, NewForestPark
                   'Officer name': 'other_fields.case_officer',
                   'Applicant surname/Company name': 'other_fields.applicant_name',

                   # conditions:
                   #'Decision': 'other_fields.decision',
                   #'Decision date': 'other_fields.decision_issued_date',

                   # dates:
                   #'Registration date': 'other_fields.date_validated',  # duplicated: CannockChase.
                   #'Validated date': 'other_fields.date_validated', # duplicated: Pembrokeshire
                   #'Decision date': 'other_fields.decision_issued_date', # duplicated: CannockChase, Pembrokeshire
                   'Consultation expiry': 'other_fields.consultation_end_date', # CannockChase.
                   'Consultation expiry date': 'other_fields.consultation_end_date', #  CannockChase*, Pembrokeshire
                   'Received date': 'other_fields.date_received', # CannockChase.
                   'Site notice date': 'other_fields.site_notice_start_date', #  CannockChase
                   'Newspapers': 'other_fields.newspapers', #  CannockChase
                   'Press notice start date': 'other_fields.press_notice_start_date', # CannockChase
                   'Press notice end date': 'other_fields.press_notice_end_date', # Pembrokeshire
                   #'Appeal lodged date': 'other_fields.appeal_lodged_date', # duplicated: CannockChas, Pembrokeshire
                   #'Appeal decision date': 'other_fields.appeal_decision_date', # duplicated: CannockChas, Pembrokeshire
                   }

    def scrape_data_items_from_AngularJS(self, app_df, item_list):
        contact_value = None
        for item in item_list:
            item_name = item.find_element(By.XPATH, './label').text.strip()
            if item_name in ['Officer telephone']:
                contact_value = item.find_element(By.XPATH, './input').get_attribute('value').strip()
            else:
                data_name = self.details_dict[item_name]
                try:
                    item_value = item.find_element(By.XPATH, './input | ./textarea').get_attribute('value').strip()
                except NoSuchElementException:
                    item_value = item.find_element(By.XPATH, './span | ./a').text.strip()

                try:
                    app_df.at[data_name] = item_value
                    print(f'    <{item_name}> scraped: {app_df.at[data_name]}') if PRINT else None
                except KeyError:
                    app_df[data_name] = item_value
                    print(f'    <{item_name}> scraped (new): {app_df.at[data_name]}') if PRINT else None
        return app_df, contact_value

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

    def parse_data_item_Agile(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta['driver']
        scraper_name = app_df.at['scraper_name']
        folder_name = self.setup_storage_path(app_df)
        max_file_name_len = self.max_folder_file_name_len - len(folder_name) - 5  # 5 chars for suffix/extension, such as .pdf
        print(f'parse_data_item_newScraper, scraper name: {scraper_name}, max_file_name_len: {max_file_name_len}.')

        try:
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="header"]/sas-cookie-consent/section/section/div[1]/button[1]'))).click()
            print('Click: Accept.')
        except TimeoutException:
            print('No Cookie button.')

        try:
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="applicationDetails"]/uib-accordion/div')))  # role = 'tablist'
        except TimeoutException:
            # Planning Application details not available.
            note = response.xpath('//*[@id="main-content"]/article/h1/text()').get()
            print('note: ', note)
            return

        tab_list = tab_panel.find_elements(By.XPATH, './div')
        for tab_index, tab in enumerate(tab_list):
            tab_name = tab.find_element(By.XPATH, './div/h4/a/span').text.strip()
            # check if the panel is opened, otherwise, data in this panel is not accessible.
            if 'panel-open' not in tab.get_attribute('class'):
                #print(f'<panel-open> not in tab: {tab_name}')
                tab.find_element(By.XPATH, './div/h4/a').click()
                time.sleep(2)
            # --- --- --- Summary (data + optional: csv) --- --- ---
            if 'summary' in tab_name.lower():
                # summaryTab = tab.div[2]/div/summaryc/div
                item_list = driver.find_elements(By.XPATH, '//*[@id="summaryTab"]/form/div')
                print(f'\n{tab_index + 1}. {tab_name} Tab: {len(item_list)} items.')
                item_list = [item.find_element(By.XPATH, './div/*/div/div') for item in item_list]
                app_df, contact_value = self.scrape_data_items_from_AngularJS(app_df, item_list)

                if contact_value:
                    contact_dict = {'officer': [app_df.at['other_fields.case_officer']],
                                    'telephone': [contact_value]}
                    contact_df = pd.DataFrame(contact_dict)
                    contact_df.to_csv(f"{self.data_storage_path}{folder_name}/contacts.csv", index=False)
            # --- --- --- Consultations (csv) --- --- ---
            elif 'consultation' in tab_name.lower():
                print(f'\n{tab_index + 1}. {tab_name} Tab.')
                n_comments = re.findall(r'\(\s*(\d+)\s*\)', tab_name)[0]
                app_df.at['other_fields.n_comments'] = n_comments

            # --- --- --- Responses (csv) --- --- ---
            elif 'responses' in tab_name.lower():
                print(f'\n{tab_index + 1}. {tab_name} Tab.')
                n_responses = re.findall(r'\(\s*(\d+)\s*\)', tab_name)[0]
                assert n_responses == 0

            # --- --- --- Constraints/Policies (csv) --- --- ---
            elif 'constraint' in tab_name.lower():
                # n_constraints = re.findall(r'\(\s*(\d+)\s*\)', tab_name)[0]
                item_table = driver.find_element(By.XPATH, '//*[@id="constraintsSection"]/section[2]/sas-table/div[2]/table/tbody')
                items = item_table.find_elements(By.XPATH, './tr')  #[1:]
                print(f'\n{tab_index + 1}. {tab_name} Tab: {len(items)} items.')
                if len(items) == 0:
                    app_df.at['other_fields.n_constraints'] = 0
                else:
                    column_name = 'Description'
                    path = 'td/span'

                    csv_name = items[0].find_element(By.XPATH, './td/a/strong').get_attribute('innerText').strip().lower()
                    table_content = []
                    for item in items[1:]:
                        try:
                            table_content.append(item.find_element(By.XPATH, f'./{path}').get_attribute('innerText').strip())
                        except NoSuchElementException:
                            # save the current csv file:
                            content_dict = {column_name: table_content}
                            content_df = pd.DataFrame(content_dict)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/{csv_name}.csv", index=False)
                            n_content = len(table_content)
                            if csv_name == 'constraints':
                                app_df.at['other_fields.n_constraints'] = n_content
                            print(f'    {csv_name}: {n_content} items.')
                            # initialize for the next csv file:
                            csv_name = item.find_element(By.XPATH, './td/a/strong').get_attribute('innerText').strip().lower()
                            table_content = []
                    # save the final csv file:
                    content_dict = {column_name: table_content}
                    content_df = pd.DataFrame(content_dict)
                    content_df.to_csv(f"{self.data_storage_path}{folder_name}/{csv_name}.csv", index=False)
                    print(f'    {csv_name}: {len(table_content)} items.')
            # --- --- --- Documents (doc) --- --- ---
            elif 'document' in tab_name.lower():
                # An external doc url:
                try:
                    # open doc url
                    panel_tab = driver.current_window_handle
                    driver.find_element(By.XPATH, '//*[@id="documentsTab"]/div/a').click()
                    time.sleep(1)
                    all_tabs = driver.window_handles
                    doc_tab = [x for x in all_tabs if x != panel_tab][0]
                    driver.switch_to.window(doc_tab)  # move to doc tab.

                    version = 2024
                    # //*[@id="searchResult_info"]
                    try:
                        documents_str = driver.find_element(By.XPATH, '//*[@id="searchResult_info"]').text.strip()  # documents_str = 'Showing 1 to 10 of {n_documents} entries'
                        documents_str = documents_str.split('of')[1]  # documents_str = '{n_documents} entries'
                        n_documents = int(re.search(r"\d+", documents_str).group())
                    except AttributeError:
                        try:
                            # //*[@id="PanelMain"]/div[1]
                            documents_str = response.css('div.TitleLabel').xpath('./text()').get()  # documents_str = 'Search Results - {n_documents} records found'
                            n_documents = int(re.search(r"\d+", documents_str).group())
                            version = 2009
                        except TypeError:
                            print('No documents are available.') if PRINT else None
                            n_documents = 0

                    print(f'\n{tab_index+1}. Documents Tab <NEC mode (ver.{version})>: {n_documents} items, folder_name: {folder_name}')
                    #print(f"{app_df.name} <NEC mode (ver.{version})> n_documents: {n_documents}, folder_name: {folder_name}")
                    app_df.at['other_fields.n_documents'] = n_documents

                    ### download documents ###
                    if n_documents > 0:
                        file_urls, document_names = get_NEC_or_Northgate_documents(driver, n_documents, self.data_upload_path, folder_name, max_file_name_len, version)
                        item = self.create_item(driver, folder_name, file_urls, document_names)
                        yield item

                    # close doc tab, back to panel tab:
                    driver.close()  # close doc tab.
                    driver.switch_to.window(panel_tab)
                # No external doc url, use doc table directly.
                except NoSuchElementException:
                    n_documents = int(re.findall(r'\(\s*(\d+)\s*\)', tab_name)[0])
                    print(f'\n{tab_index + 1}. Documents Tab: {n_documents} items, folder_name: {folder_name}')
                    app_df.at['other_fields.n_documents'] = n_documents
                    if n_documents > 0:
                        n_documents = 0
                        item_table = driver.find_element(By.XPATH, '//*[@id="documents"]/div[2]/table/tbody')
                        item_list = item_table.find_elements(By.XPATH, './tr')
                        document_type, document_description, document_date, file_urls, document_names  = None, None, None, [], []
                        LA_abbreviation = driver.current_url.split('/')[-3].upper()
                        print('LA abbreviation: ', LA_abbreviation)
                        for item_index, item in enumerate(item_list):
                            row_data = driver.execute_script("return angular.element(arguments[0]).scope().row;", item)
                            if row_data:
                                n_documents += 1

                                file_url = f"https://planningapi.agileapplications.co.uk//api/application/document/{LA_abbreviation}/{row_data['documentHash']}"
                                print('file url: ', file_url)
                                file_urls.append(file_url)
                                document_extension = row_data['name'].split('.')[-1].strip()
                                document_name = f"date={row_data['receivedDate'][:10]}&type={row_data['mediaDescription']}&desc={row_data['description']}&uid={n_documents}.{document_extension}"
                                print(f'    Document {n_documents}: {document_name}') if PRINT else None
                                len_limitation = len(document_name) - max_file_name_len
                                print(f'    Doc {n_documents} len_limitation: {len_limitation}') if len_limitation > -5 else None
                                if len_limitation > 0:
                                    temp_name = document_name.split('&uid')[0]
                                    document_name = f'{temp_name[:-len_limitation]}&uid={n_documents}.{document_extension}'
                                document_name = replace_invalid_characters(document_name)
                                document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                        item = self.create_item(driver, folder_name, file_urls, document_names)
                        yield item

            # --- --- --- Conditions (data + csv) --- --- ---
            elif 'condition' in tab_name.lower():
                n_conditions = re.findall(r'\(\s*(\d+)\s*\)', tab_name)[0]
                item_list = driver.find_elements(By.XPATH, '//*[@id="conditionsTab"]/div/form/div')
                # //*[@id="conditionsTab"]/div/form/div[1]/sas-input-text/div/div
                print(f'\n{tab_index + 1}. {tab_name} Tab: {len(item_list)} items + {n_conditions} conditions.')
                item_list = [item.find_element(By.XPATH, './*/div/div') for item in item_list]
                app_df, _ = self.scrape_data_items_from_AngularJS(app_df, item_list)

                if n_conditions != '0':
                    csv_name = 'conditions'
                    content_dict = {}
                    item_table = driver.find_element(By.XPATH, '//*[@id="conditionsTab"]/section[2]/sas-table/div[1]/table/tbody')
                    items = item_table.find_elements(By.XPATH, './tr')

                    column_names = [column.get_attribute('data-title').strip() for column in items[0].find_elements(By.XPATH, './td')]
                    column_names = unique_columns(column_names)
                    n_columns = len(column_names)

                    for column_index in range(n_columns):
                        content_dict[column_names[column_index]] = [table_item.find_element(By.XPATH, f'./td[{column_index+1}]/span').get_attribute('innerText').strip() for table_item in items]

                    content_df = pd.DataFrame(content_dict)
                    content_df.to_csv(f'{self.data_storage_path}{folder_name}/{csv_name}.csv', index=False)
                    # //*[@id="conditionsTab"]/section[2]/sas-table/div[1]/table/tbody/tr/td[1]/span
            # --- --- --- Dates (data) --- --- ---
            elif 'date' in tab_name.lower():
                item_list = driver.find_elements(By.XPATH, '//*[@id="datesTab"]/form/div')
                # //*[@id="datesTab"]/form/div[1]/div/sas-input-text/div/div
                print(f'\n{tab_index + 1}. {tab_name} Tab: {len(item_list)} items.')
                item_list = [item.find_element(By.XPATH, './div/*/div/div') for item in item_list]
                app_df, _ = self.scrape_data_items_from_AngularJS(app_df, item_list)

            # --- --- --- Map --- --- ---
            elif 'map' in tab_name.lower():
                print(f'\n{tab_index + 1}. {tab_name} Tab.')
            else:
                print(f'\n{tab_index+1}. Unknown Tab: {tab_name}.')
                assert 1 == 0
        self.ending(app_df)