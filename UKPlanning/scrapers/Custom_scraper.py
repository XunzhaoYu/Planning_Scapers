import os, time, random, re
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
from general.utils import unique_columns, scrape_data_items, scrape_for_csv, scrape_multi_tables_for_csv # test for further re-organization.


class Custom_Scraper(Base_Scraper):
    name = 'Custom_Scraper'
    """
    auth_id = 412(408), Wiltshire: https://development.wiltshire.gov.uk/pr/s/planning-application/a0i3z000014eboiAAA
                        This LA portal was updated in 2025⚠️. Scraper was revised on 10th-Nov-2025 for this update.
    """

    # use pipelines_extension to obtain file extensions.
    custom_settings = {'ITEM_PIPELINES': {'UKPlanning.pipelines.pipelines_extension.DownloadFilesPipeline': 1, }}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All sub_classes of Base_Scraper should define their self.parse_func(s) in __init__
        self.parse_func = self.parse_data_item_Wiltshire

    details_dict = {'Planning Application Name': 'uid',
                    'Application Type': 'other_fields.application_type',
                    'Officer Name': 'other_fields.case_officer',

                    'Valid Date': 'other_fields.date_validated',
                    'Consultation Deadline': 'other_fields.consultation_end_date',
                    'Date of Committee': 'other_fields.meeting_date',
                    'Acknowledged Date': 'other_fields.acknowledged_date',  # new

                    'Latest Decision Date (Calculated)': 'other_fields.decision_date',
                    'Issued Decision': 'other_fields.decision',
                    'Decision Notice Sent Date': 'other_fields.decision_notice_date',
                    'Current Decision Expiry Date': 'other_fields.decision_due_date',

                    'Wards': 'other_fields.ward_name',
                    'Parishes': 'other_fields.parish'
                    }

    # moved to general.utils.  test for further re-organization.
    """
    def scrape_data_items(self, app_df, items, item_values):
        for item, value in zip(items, item_values):
            item_name = item.text.strip()
            data_name = self.details_dict[item_name]
            item_value = value.text.strip()
            # print(i, item_name, item_value, type(item_name)) 
            try:
                app_df.at[data_name] = item_value
                print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
            # New
            except KeyError:
                app_df[data_name] = item_value
                print(f"    <{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
        return app_df 
    #"""

    # Not used. The doc table uses a mix of th and td, not td only.
    # For the same reason, comments are scraped without existing utils, e,g, scrape_data_items
    """
    def rename_document(self, document_item, document_name, description_column=2, type_column=1, date_column=3, path='td'):
        try:
            document_description = document_item.find_element(By.XPATH, f'./{path}[{description_column}]').text.strip()
            document_name = f"desc={document_description}&{document_name}"
        except NoSuchElementException:
            pass
        document_type = document_item.find_element(By.XPATH, f'./{path}[{type_column}]').text.strip()
        document_name = f"type={document_type}&{document_name}"
        try:
            document_date = document_item.find_element(By.XPATH, f'./{path}[{date_column}]').text.strip()
            document_name = f"date={document_date}&{document_name}"
        except NoSuchElementException:
            pass
        return document_name
    #"""

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

    def get_column_indexes(self, columns, keywords):
        n_columns = len(columns)
        n_keywords = len(keywords)
        column_indexes = [n_columns + 1, ] * n_keywords
        for keyword_index, keyword in enumerate(keywords):
            for column_index, column in enumerate(columns):
                if keyword in str.lower(column.text.strip()):
                    column_indexes[keyword_index] = column_index + 1
                    break
        if PRINT:
            print_str = '    Columns: '
            for keyword_index, keyword in enumerate(keywords):
                print_str += f'{keyword} column {column_indexes[keyword_index]}/{n_columns}. '
            print(print_str)
        return column_indexes

    def parse_data_item_Wiltshire(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta['driver']
        scraper_name = app_df.at['scraper_name']
        folder_name = self.setup_storage_path(app_df)
        max_file_name_len = self.max_folder_file_name_len - len(folder_name) - 5  # 5 chars for suffix/extension, such as .pdf
        print(f'parse_data_item_Custom, scraper name: {scraper_name}, max_file_name_len: {max_file_name_len}.')

        try:
            # --- --- --- Main Details --- --- ---
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@data-region-name="1"]')))
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
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  # scroll down to the bottom of this page.

        # working before LA portal update. ⚠️
        """ 
        dl_list = tab_panel.find_elements(By.CLASS_NAME, 'slds-grid.slds-gutters_small.full.cols-2.forcePageBlockSectionRow')
        print(f'dl list: {len(dl_list)}')
        # items = tab_panel.find_elements(By.CLASS_NAME, 'test-id__field-label')
        # item_values = tab_panel.find_elements(By.CLASS_NAME, 'test-id__field-value')
        items = [dl.find_element(By.XPATH, './div[1]/div/dt') for dl in dl_list]
        item_values = [dl.find_element(By.XPATH, './div[1]/div/dd') for dl in dl_list]
        #"""
        # new scraper module after LA portal update
        dl_list = tab_panel.find_elements(By.CLASS_NAME, 'slds-form-element_readonly')
        print(f'dl list: {len(dl_list)}')
        items = [dl.find_element(By.XPATH, './div[1]/span') for dl in dl_list]
        item_values = [dl.find_element(By.XPATH, './div[2]/span/slot') for dl in dl_list]

        n_items = len(items)
        if n_items == 0:  # reload
            print('reload ...')
            yield SeleniumRequest(url=driver.current_url, callback=self.parse_func, meta={'app_df': app_df})
            return
        if n_items == 0:
            assert 0 == 1
        print(f"\n1. Main Details Tab: {n_items}")
        # test for further re-organization.
        #app_df = self.scrape_data_items(app_df, items, item_values)
        app_df = scrape_data_items(app_df, items, item_values, self.details_dict, PRINT)

        container = tab_panel.find_element(By.XPATH, './div[3]/div/div/div')
        appeals = container.find_element(By.XPATH, './div[1]/article/div[1]/header/div[2]/h2/a/span[2]').text.strip()
        while len(appeals) == 0:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            appeals = container.find_element(By.XPATH, './div[1]/article/div[1]/header/div[2]/h2/a/span[2]').text.strip()
        print(f'appeals: {appeals}.')
        assert(appeals == '(0)')
        planning_obligations = container.find_element(By.XPATH, './div[2]/article/div[1]/header/div[2]/h2/a/span[2]').text.strip()
        print(f'planning obligations: {planning_obligations}.')
        assert (planning_obligations == '(0)')

        # tablist: # //*[@id="contentStart"]/div/div[4]/div/div
        tabs = driver.find_elements(By.XPATH, '//*[@id="contentStart"]/div/div[4]/div/div/ul/li/a')[:-1]
        app_df = self.set_default_items(app_df)
        for tab_index, tab in enumerate(tabs[1:]):
            tab.click()
            tab_name = tab.text.strip()
            # print(f"tab {tab_index + 1}: {tab_name}")
            # --- --- --- Associate Documents (doc) --- --- ---
            if 'Documents' in tab_name:  # description uses th, date and type use td.
                def get_documents():
                    try:
                        #document_table = document_panel.find_element(By.XPATH, './/*[@role="grid"]')
                        document_table = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@data-region-name="{tab_index+2}"]/div[2]/article/div[2]/div/lightning-datatable/div[2]/div/div/table')))
                    except TimeoutException:
                        app_df.at['other_fields.n_documents'] = 0
                        print(f"\n{tab_index+2}. <NULL>  Document Tab: {app_df.at['other_fields.n_documents']} items.") if PRINT else None
                        return 0, [], []

                    document_items = document_table.find_elements(By.XPATH, './tbody/tr')
                    n_items = len(document_items)
                    print(f"\n{tab_index+2}. Document Tab: {n_items} items") if PRINT else None

                    columns = document_table.find_elements(By.XPATH, './thead/tr/th')
                    [date_column, type_column, description_column] = self.get_column_indexes(columns, keywords=['date', 'category', 'title'])
                    if description_column < date_column:
                        date_column -= 1
                    if description_column < type_column:
                        type_column -= 1

                    n_documents, file_urls, document_names = 0, [], []
                    for document_item in document_items:
                        n_documents += 1
                        #file_url = f"https://development.wiltshire.gov.uk/pr/s/contentdocument/{document_item.get_attribute('data-row-key-value')}"
                        file_url = f"https://development.wiltshire.gov.uk/pr/sfc/servlet.shepherd/document/download/{document_item.get_attribute('data-row-key-value')}?operationContext=S1"
                        print(file_url + '.')
                        file_urls.append(file_url)

                        document_name = f"uid={n_documents}"
                        try:
                            document_description = document_item.find_element(By.XPATH, f'./th').text.strip()
                            document_name = f"desc={document_description}&{document_name}"
                        except NoSuchElementException:
                            pass
                        try:
                            document_type = document_item.find_element(By.XPATH, f'./td[{type_column}]').text.strip()
                            document_name = f"type={document_type}&{document_name}"
                        except NoSuchElementException:
                            pass
                        try:
                            document_date = document_item.find_element(By.XPATH, f'./td[{date_column}]').text.strip()
                            document_name = f"date={document_date}&{document_name}"
                        except NoSuchElementException:
                            pass
                        #document_name = self.rename_document(document_item, document_name, description_column=description_column, type_column=type_column, date_column=date_column, path='th')
                        len_limitation = len(document_name) - max_file_name_len
                        print(
                            f'    Doc {n_documents} len_limitation: {len_limitation}') if len_limitation > -5 else None
                        if len_limitation > 0:
                            temp_name = document_name.split('&uid')[0]
                            document_name = f'{temp_name[:-len_limitation]}&uid={n_documents}'
                        print(f"    Document {n_documents}: {document_name}") if PRINT else None

                        document_name = replace_invalid_characters(document_name)
                        document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                    app_df.at['other_fields.n_documents'] = n_documents
                    print(f'Total documents: {n_documents}') if PRINT else None
                    return n_documents, file_urls, document_names
                n_documents, file_urls, document_names = get_documents()
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            # --- --- --- Consultee (csv) --- --- ---  # scrape_for_csv
            elif 'Comments' in tab_name:
                def parse_consultees():
                    try:
                        consultees_table = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@data-region-name="{tab_index+2}"]/div[3]/div[3]/lightning-datatable/div[2]/div/div/table')))
                        table_items = consultees_table.find_elements(By.XPATH, './tbody/tr')
                        n_items = len(table_items)
                        if n_items == 10:
                            comment_auxiliaries = driver.find_elements(By.XPATH, f'//*[@data-region-name="{tab_index+2}"]/div[3]/div[3]/div[2]/div')
                            comment_counter = comment_auxiliaries[2].text.strip()
                            print(comment_counter)
                            n_items = int(re.findall(r'\d', comment_counter)[1])
                            select = Select(comment_auxiliaries[1].find_element(By.XPATH, './div/div/div/select'))
                            select.select_by_visible_text("100 records per page")

                            consultees_table = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@data-region-name="{tab_index+2}"]/div[3]/div[3]/lightning-datatable/div[2]/div/div/table')))
                            table_items = consultees_table.find_elements(By.XPATH, './tbody/tr')
                            assert(len(table_items) == n_items)
                        print(f"\n{tab_index+2}. Consultees Tab: {n_items}") if PRINT else None
                        if n_items > 0:
                            app_df.at['other_fields.n_comments_consultee_responded'] = n_items
                            app_df.at['other_fields.n_comments'] = n_items
                            table_columns = consultees_table.find_elements(By.XPATH, './thead/tr/th')

                            content_dict = {}
                            column_names = [column.find_element(By.XPATH, './lightning-primitive-header-factory/span/a/span[2]').text.strip() for column in table_columns]
                            column_names = unique_columns(column_names)
                            # print(f'{table_name}, {len(table_items)} items with column names: ', column_names) if PRINT else None
                            n_columns = len(column_names)
                            assert n_columns == 3
                            content_dict[column_names[0]] = [table_item.find_element(By.XPATH, f'./th').text.strip() for table_item in table_items]
                            content_dict[column_names[1]] = [table_item.find_element(By.XPATH, f'./td[1]').text.strip() for table_item in table_items]
                            content_dict[column_names[2]] = [table_item.find_element(By.XPATH, f'./td[2]').text.strip() for table_item in table_items]

                            content_df = pd.DataFrame(content_dict)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/comments.csv", index=False)
                            #self.scrape_for_csv(csv_name='comments', table_columns=table_columns, table_items=table_items, folder_name=folder_name, path='td')
                    except TimeoutException:  # No Comments found for this Application
                        #print(f"\n{tab_index+2}. " + driver.find_element(By.XPATH, f'//*[@data-region-name="{tab_index+2}"]/div[3]/p').text.strip())
                        print(f"\n{tab_index+2}. " + WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f'//*[@data-region-name="{tab_index+2}"]/div[3]/p'))).text.strip())
                parse_consultees()
            # --- --- --- Unknown Tabs --- --- ---
            else:
                print(f'\n{tab_index+2}. Unknown tab: {tab_name}')
                assert 0 == 1
        self.ending(app_df)