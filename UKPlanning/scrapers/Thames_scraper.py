import os, time, random, re
import pandas as pd

from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException, ElementClickInterceptedException

from configs.settings import PRINT
from general.base_scraper import Base_Scraper
from general.document_utils import replace_invalid_characters, get_documents
from general.items import DownloadFilesItem
from general.utils import unique_columns, scrape_data_items, scrape_for_csv, scrape_multi_tables_for_csv


class Thames_Scraper(Base_Scraper):
    name = 'Thames_Scraper'
    """
    auth_id = 297(294), Richmond: https://www2.richmond.gov.uk/lbrplanning/Planning_CaseNo.aspx?strCASENO=10/1112/NMA
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All sub_classes of Base_Scraper should define their self.parse_func(s) in __init__
        self.parse_func = self.parse_data_item_Richmond

    details_dict = {'Site address': 'address',
                    'Status': 'other_fields.status',
                    # Details
                    'Proposal': 'description',
                    'Type': 'other_fields.application_type',
                    'Ward': 'other_fields.ward_name',
                    'Applicant': 'other_fields.applicant_name',
                    "Applicant's Address": 'other_fields.applicant_address',
                    'Agent': 'other_fields.agent_name',
                    "Agent's Address": 'other_fields.agent_address',
                    'Officer': 'other_fields.case_officer',
                    # Progress
                    'Application Received': 'other_fields.date_received',
                    'Validated': 'other_fields.date_validated',
                    'Neighbour notification started': 'other_fields.neighbour_notification_started',
                    'Neighbour notification ended': 'other_fields.neighbour_notification_ended',
                    'Decision Issued': 'other_fields.decision_issued_date',
                    }

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

    def parse_data_item_Richmond(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta['driver']
        scraper_name = app_df.at['scraper_name']
        folder_name = self.setup_storage_path(app_df)
        max_file_name_len = self.max_folder_file_name_len - len(folder_name) - 5  # 5 chars for suffix/extension, such as .pdf
        print(f'parse_data_item_newScraper, scraper name: {scraper_name}, max_file_name_len: {max_file_name_len}.')

        try:
            # --- --- --- Main Details --- --- ---
            tab_panel = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, '//*[@id="aspnetForm"]')))
        except TimeoutException:  # ***** situation to be completed: application not available
            """
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
            #"""
            print('*** *** *** This application is not available. *** *** ***')
            return

        app_df.at['address'] = tab_panel.find_element(By.XPATH, '//*[@id="ctl00_PageContent_lbl_Site_description"]').text.strip()
        print(f"    <Site Address> scraped: {app_df.at['address']}") if PRINT else None
        if len(app_df.at['other_fields.status']) > 0:
            print(f"    <Status> already filled: {app_df.at['other_fields.status']}") if PRINT else None
        else:
            app_df.at['other_fields.status'] = tab_panel.find_element(By.XPATH, '//*[@id="ctl00_PageContent_lbl_Status"]').text.strip()
            print(f"    <Status> scraped: {app_df.at['other_fields.status']}") if PRINT else None

        print("\nDetails:")
        container = tab_panel.find_element(By.XPATH, './div[5]/div')
        details = container.find_element(By.XPATH, './div[2]/div')
        try:
            show_applicant_details_button = details.find_element(By.XPATH, '//*[@id="ctl00_PageContent_btnShowApplicantDetails"]')
            show_applicant_details_button.click()
            print('show applicant details')
            tab_panel = WebDriverWait(driver, 10).until( EC.visibility_of_element_located((By.XPATH, '//*[@id="aspnetForm"]')))
            container = tab_panel.find_element(By.XPATH, './div[5]/div')
            details = container.find_element(By.XPATH, './div[2]/div')
        except ElementClickInterceptedException:
            if 'cookie' in driver.find_element(By.XPATH, '//*[@id="ccc-title"]').text.strip():
                print('Reject cookie.')
                driver.find_element(By.XPATH, '//*[@id="ccc-reject-settings"]').click()

                show_applicant_details_button = details.find_element(By.XPATH, '//*[@id="ctl00_PageContent_btnShowApplicantDetails"]')
                show_applicant_details_button.click()
                print('show applicant details')
                tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="aspnetForm"]')))
                container = tab_panel.find_element(By.XPATH, './div[5]/div')
                details = container.find_element(By.XPATH, './div[2]/div')
            else:
                assert 1 == 0
        except NoSuchElementException:
            time.sleep(2)
            if 'cookie' in driver.find_element(By.XPATH, '//*[@id="ccc-title"]').text.strip():
                print('Reject cookie.')
                driver.find_element(By.XPATH, '//*[@id="ccc-reject-settings"]').click()

        detail_items = details.text.strip().split('\n')
        print(type(detail_items), detail_items)
        assert detail_items[-2] == 'Site plan'
        item_scraped = False
        for item in detail_items[:-2]:
            try:
                data_name = self.details_dict[item]
                item_name = item
                # print(f"    item name: {item_name}, data name: {data_name}")
            except KeyError:
                if item_name in ['Applicant', 'Agent']:
                    if item_scraped:
                        item_name = item_name + "'s Address"
                        data_name = self.details_dict[item_name]
                        item_scraped = False
                    else:
                        item_scraped = True
                        match = re.search(r'\d', item)
                        if match:
                            split_index = match.start()
                            item_name2 = item_name + "'s Address"
                            data_name2 = self.details_dict[item_name2]
                            app_df.at[data_name2] = item[split_index:]
                            print(f"    <{item_name2}> scraped: {app_df.at[data_name2]}") if PRINT else None
                            item = item[:split_index].strip()
                app_df.at[data_name] = item
                print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None

        # --- --- --- Progress (data) --- --- ---
        print("\nProgress:")
        progress_items = container.find_elements(By.XPATH, './div[5]/div/ul/li')
        for item in progress_items:
            item = item.text.split(':')
            item_name = item[0].strip()
            data_name = self.details_dict[item_name]
            item_value = item[1].strip()
            app_df.at[data_name] = item_value
            print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None

        # --- --- --- Documents (doc) --- --- ---
        def get_documents(n_documents, file_urls, document_names, document_type, folder_name):
            document_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="ctl00_PageContent_grdDocuments"]')))
            document_items = document_list.find_elements(By.XPATH, './tbody/tr')[1:]
            for document_item in document_items:
                n_documents += 1

                file_button = document_item.find_element(By.XPATH, f'./td/a')
                current_tab = driver.current_window_handle
                # print(f'current tab: {current_tab}')
                try:
                    file_button.click()
                    time.sleep(2)
                    all_tabs = driver.window_handles
                    new_tabs = [x for x in all_tabs if x != current_tab]
                    if len(new_tabs) == 0:
                        print(f'file url: NULL, new tab already closed.')
                        assert 1 == 0  # fix it later.
                        continue
                    else:
                        new_tab = new_tabs[0]
                        driver.switch_to.window(new_tab)
                        file_url = driver.current_url  # https://planning2.wandsworth.gov.uk/iam/IAMCache/439852/439852.pdf
                        print('file url: ', file_url)
                        if file_url == 'about:blank':
                            print(f'    Document {n_documents}: Skip.')
                            print(f'    app tab: {current_tab}')
                            print(f'    all tabs: {driver.window_handles}')
                            print(f'    doc tab: {new_tab}')
                            while new_tab in driver.window_handles:
                                time.sleep(1)
                            # print(f'current tab: {driver.current_window_handle}')
                            driver.switch_to.window(current_tab)
                            continue
                        else:
                            file_urls.append(file_url)
                            driver.close()
                            driver.switch_to.window(current_tab)
                except ConnectionRefusedError:
                    print(f'    Document {n_documents}: Skip, Connection Refused Error.')
                    continue
                except ElementNotInteractableException:
                    # """
                    file_id = document_item.find_element(By.XPATH, './td/a').get_attribute('href')
                    file_id = file_id.split('docid=')[1]
                    file_url = f"https://images.richmond.gov.uk/iam/IAMCache/{file_id}/{file_id}.pdf"
                    file_urls.append(file_url)
                    # """

                doc_extension = file_url.split('.')[-1]
                document_name = f"uid={n_documents}.{doc_extension}"
                document_description = document_item.find_element(By.XPATH, './td[1]').text.strip()
                document_name = f"desc={document_description}&{document_name}"
                document_name = f"type={document_type}&{document_name}"
                document_date = document_item.find_element(By.XPATH, './td[2]').text.strip()
                document_name = f"date={document_date}&{document_name}"
                len_limitation = len(document_name) - max_file_name_len
                print(
                    f'    Doc {n_documents} len_limitation: {len_limitation}') if len_limitation > -5 else None
                if len_limitation > 0:
                    temp_name = document_name.split('&uid')[0]
                    document_name = f'{temp_name[:-len_limitation]}&uid={n_documents}.{doc_extension}'

                print(f"    Document {n_documents}: {document_name}") if PRINT else None
                document_name = replace_invalid_characters(document_name)
                document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
            return n_documents, file_urls, document_names

        n_documents, file_urls, document_names = 0, [], []
        sub_tables = container.find_elements(By.XPATH, './*[@id="ctl00_PageContent_divDocumentsAndImages"]/div/div/ul/li')
        n_sub_tables = len(sub_tables)
        print("Number of sub tables: ", n_sub_tables)
        for i in range(n_sub_tables):
            document_tab = driver.find_element(By.XPATH, f'//*[@id="ctl00_PageContent_divDocumentsAndImages"]/div/div/ul/li[{i+1}]/a')
            document_type = document_tab.text.strip()
            print(i, ": ", document_type)
            document_tab.click()
            n_documents, file_urls, document_names = get_documents(n_documents, file_urls, document_names, document_type, folder_name)

        app_df.at['other_fields.n_documents'] = n_documents
        print(f'\nDocuments: {n_documents} files.') if PRINT else None
        if n_documents > 0:
            item = self.create_item(driver, folder_name, file_urls, document_names)
            yield item

        self.ending(app_df)