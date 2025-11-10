import os, time, random
import pandas as pd

from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from configs.settings import PRINT
from general.base_scraper import Base_Scraper
from general.document_utils import replace_invalid_characters
from general.items import DownloadFilesItem
from general.utils import scrape_data_items, scrape_for_csv  # test for further re-organization.
#from tools.reCAPTCHA.reCAPTCHA_model import predict_base64_image
from tools.reCAPTCHA.reCAPTCHA_API import solve_puzzle, click_puzzle_buttons


class Tascomi_Scaper(Base_Scraper):
    name = 'Tascomi_Scraper'
    """
    auth_id = 17, Barking: http://paplan.lbbd.gov.uk/online-applications/applicationDetails.do?activeTab=summary&keyVal=0100002PRIOR
                    [early years unavailable] https://online-befirst.lbbd.gov.uk/planning/index.html?fa=getApplication&id=9134
    auth_id = 69, Ceredigion: https://ceredigion-online.tascomi.com/planning/index.html?fa=getApplication&id=36520
    auth_id = 90, Coventry: https://planningsearch.coventry.gov.uk/planning/application/556153 [X]
                    [Search ID] https://planandregulatory.coventry.gov.uk/planning/index.html?fa=getApplication&id=249328
    auth_id = 98, Dartmoor: https://dartmoor-online.tascomi.com/planning/index.html?fa=getApplication&id=126932
    auth_id = 161(159), Gwynedd: https://amg.gwynedd.llyw.cymru/planning/index.html?fa=getApplication&id=9310
    auth_id = 162(160), Hackney: https://developmentandhousing.hackney.gov.uk/planning/index.html?fa=getApplication&id=63606
    *auth_id = 171(169), Harrow: https://planningsearch.harrow.gov.uk/planning/planning-application?RefType=GFPlanning&KeyNo=696420
    #   [404 Not Found, Search ID] https://planningsearch.harrow.gov.uk/planning/index.html?fa=getApplication&id=131697
    auth_id = 215(213): Liverpool: https://lar.liverpool.gov.uk/planning/index.html?fa=getApplication&id=137542
    auth_id = 242(240), NewcastleUponTyne: https://portal.newcastle.gov.uk/planning/index.html?fa=getApplication&id=73907
    auth_id = 387(383), WalthamForest: https://builtenvironment.walthamforest.gov.uk/planning/index.html?fa=getApplication&id=23739
    auth_id = 389(385), Warrington: https://online.warrington.gov.uk/planning/index.html?fa=getApplication&id=167271
    auth_id = 415(411), Wirral: https://online.wirral.gov.uk/planning/index.html?fa=getApplication&id=160647
    """

    # use pipelines_extension to obtain file extensions.
    custom_settings = {'ITEM_PIPELINES': {'UKPlanning.pipelines.pipelines_extension.DownloadFilesPipeline': 1,},
                       'SELENIUM_DRIVER_ARGUMENTS': []}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All sub_classes of Base_Scraper should define their self.parse_func(s) in __init__
        if self.auth in ['Coventry']:
            self.parse_func = self.parse_Conventry_search_page_Tascomi
        elif self.auth in ['Harrow']:
            self.parse_func = self.parse_Harrow_search_page_Tascomi
        else:
            self.parse_func = self.parse_data_item_Tascomi

    # Note: these item names are ending with ':'.
    details_dict = {'Application Reference Number:': 'uid',
                    'Application Type:': 'other_fields.application_type',
                    'Proposal:': 'description',

                    'Applicant:': 'other_fields.applicant_name',
                    'Agent:': 'other_fields.agent_name',
                    'Location:': 'address',
                    'Grid Reference:': 'other_fields.grid_reference',  # New
                    'Ward:': 'other_fields.ward_name',
                    'Parish / Community:': 'other_fields.parish',
                    'Parish / Parish:': 'other_fields.parish',  # Warrington
                    'Officer:': 'other_fields.case_officer',
                    'Decision Level:': 'other_fields.expected_decision_level',
                    'Application Status:': 'other_fields.status',

                    'Received Date:': 'other_fields.date_received',
                    'Valid Date:': 'other_fields.date_validated',
                    'Expiry Date:': 'other_fields.application_expires_date',

                    'Extension Of Time:': 'other_fields.extension_of_time',  # New
                    'Extension Of Time Due Date:': 'other_fields.extension_of_time_due_date',  # New
                    'Planning Performance Agreement:': 'other_fields.planning_performance_agreement',  # New
                    'Planning Performance Agreement Due Date:': 'other_fields.planning_performance_agreement_due_date',
                    # New
                    'Proposed Committee Date:': 'other_fields.proposed_meeting_date',  # New
                    'Actual Committee Date:': 'other_fields.meeting_date',
                    'Decision Issued Date:': 'other_fields.decision_issued_date',
                    'Decision:': 'other_fields.decision',
                    'Publicity End Date:': 'other_fields.public_consultation_end_date',  # Ceredigion
                    'Consultation End Date:': 'other_fields.public_consultation_end_date',  # Gwynedd
                    # Appeal
                    'Appeal Reference:': 'other_fields.appeal_reference',
                    'Appeal Status:': 'other_fields.appeal_status',
                    'Appeal External Decision:': 'other_fields.appeal_result',
                    'Appeal External Decision Date:': 'other_fields.appeal_decision_date',
                    }

    # moved to general.utils.  test for further re-organization.
    """
    def scrape_data_items(self, app_df, items, item_values):
        for item, value in zip(items, item_values):
            item_name = item.text.strip()  #[:-1]
            print(f'item_name: {item_name}') if PRINT else None
            data_name = self.details_dict[item_name]
            item_value = value.text.strip()
            try:
                app_df.at[data_name] = item_value
                print(f'    <{item_name}> scraped: {app_df.at[data_name]}') if PRINT else None
            # New
            except KeyError:
                app_df[data_name] = item_value
                print(f'    <{item_name}> scraped (new): {app_df.at[data_name]}') if PRINT else None
        return app_df

    def scrape_for_csv(self, csv_name, table_columns, table_items, folder_name, path='td'):
        content_dict = {}
        column_names = [column.text.strip() for column in table_columns]
        # column_names = self.unique_columns(column_names)
        n_columns = len(column_names)

        for column_index in range(n_columns):
            content_dict[column_names[column_index]] = [table_item.find_element(By.XPATH, f'./{path}[{column_index + 1}]').text.strip() for table_item in table_items]

        content_df = pd.DataFrame(content_dict)
        content_df.to_csv(f'{self.data_storage_path}{folder_name}/{csv_name}.csv', index=False)
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

    def parse_Conventry_search_page_Tascomi(self, response):
        app_df = response.meta['app_df']
        url = 'https://planandregulatory.coventry.gov.uk/planning/index.html?fa=search'
        yield SeleniumRequest(url=url, callback=self.search_by_appID_Tascomi, meta={'app_df': app_df})

    def parse_Harrow_search_page_Tascomi(self, response):
        app_df = response.meta['app_df']
        url = 'https://planningsearch.harrow.gov.uk/planning/index.html?fa=search'
        yield SeleniumRequest(url=url, callback=self.search_by_appID_Tascomi, meta={'app_df': app_df})

    # A module to search applications using their app_id.
    def search_by_appID_Tascomi(self, response):
        driver = response.request.meta['driver']
        app_df = response.meta['app_df']
        current_tab = driver.current_window_handle
        url = response.request.url
        print(f'search page url: {url}') if PRINT else None

        # use app_id to search and view the application page.
        app_id = app_df.at['uid']
        input_reference = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, 'application_reference_number')))
        input_reference.click()
        input_reference.send_keys(app_id)
        # click 'search' button.
        driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')  # scroll down to the bottom of this page.
        time.sleep(random.uniform(1., 1.5))
        driver.find_element(By.CLASS_NAME, 'btn-success').click()
        # click 'view' button.
        try:
            view_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="application_results_table"]/tbody/tr/td[8]/button')))
            time.sleep(random.uniform(.5, 1.))
            view_button.click()
        except TimeoutException:
            print(f'Timeout error - search result: Application {app_id} is not found.')
            return
        time.sleep(random.uniform(4., 5.))

        # move to the new tab: application page
        all_tabs = driver.window_handles
        new_tab = [x for x in all_tabs if x != current_tab][0]
        driver.close() # close the 'search page' tab.
        driver.switch_to.window(new_tab) # move to new tab.
        print(f'update actual url: {driver.current_url}')
        app_df.at['url'] = driver.current_url

        # scrape application
        yield from self.parse_data_item_Tascomi(response)

    def parse_data_item_Tascomi(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta['driver']
        scraper_name = app_df.at['scraper_name']
        folder_name = self.setup_storage_path(app_df)
        max_file_name_len = self.max_folder_file_name_len - len(folder_name) - 5  # 5 chars for suffix/extension, such as .pdf
        print(f'parse_data_item_Tascomi, scraper name: {scraper_name}, max_file_name_len: {max_file_name_len}.')
        try:
            reCAPTCHA_start_buttion = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="amzn-captcha-verify-button"]')))
            reCAPTCHA_start_buttion.click()
            print(' --- --- --- solving reCAPTCHA puzzle --- --- --- ')
            solution = solve_puzzle(driver)
            print(f' --- --- --- reCAPTCHA puzzle solution: {solution} --- --- --- ')
            click_puzzle_buttons(driver, solution)
        except TimeoutException:
            pass

        time.sleep(random.uniform(4., 5.))
        try:
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="content"]/div/section')))
        except TimeoutException:
            # Planning Application details not available . e.g. auth=3, year=[15:18]
            note = response.xpath('//*[@id="main-content"]/article/h1/text()').get()
            print('note: ', note)
            return
        driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')  # scroll down to the bottom of this page.

        tab_list = tab_panel.find_elements(By.XPATH, './div/article/div')
        n_tabs = len(tab_list)
        file_urls, document_names = [], []  # Shared by document tab and representation tab.
        for tab_index, tab in enumerate(tab_list):
            tab_name = tab.get_attribute('id').strip()
            #print(f'\n- - - Tab {tab_index + 1}/{n_tabs}: {tab_name} - - -')
            # --- --- --- Main Details (data) --- --- ---
            if tab_name == 'application_details':
                detail_list = tab.find_elements(By.XPATH, './div/div[2]/div/div')
                items = [detail.find_element(By.XPATH, './div[1]') for detail in detail_list]
                item_values = [detail.find_element(By.XPATH, './div[2]') for detail in detail_list]
                n_items = len(items)
                print(f'\n{tab_index+1}. Details Tab: {n_items} items.')
                # test for further re-organization.
                #app_df = self.scrape_data_items(app_df, items, item_values)
                app_df = scrape_data_items(app_df, items, item_values, self.details_dict, PRINT)
            # --- --- --- Location (null) --- --- ---
            elif tab_name == 'application_map_preview':
                print(f'\n{tab_index+1}. Map.')
            # --- --- --- Associate Documents (doc) --- --- --- extract file_urls and document_names in this tab, but download files after this tab.
            elif tab_name == 'documents':
                doc_list = tab.find_element(By.XPATH, './div/div[2]/table/tbody')
                document_items = doc_list.find_elements(By.XPATH, './tr')
                n_documents = len(document_items)
                app_df.at['other_fields.n_documents'] = n_documents
                print(f'\n{tab_index+1} Documents Tab: {n_documents} items.')
                if n_documents > 0:
                    n_documents = 0  # , file_urls, document_names = 0, [], []
                    for document_item in document_items:
                        n_documents += 1
                        print(f'    - - - Document {n_documents} - - -') if PRINT else None
                        columns = document_item.find_elements(By.XPATH, './td')
                        print('    len columns: ', len(columns)) if len(columns) != 5 else None
                        assert len(columns) == 5
                        try:
                            file_url = columns[4].find_element(By.XPATH, './a').get_attribute('href')
                            print(f'    {file_url}') if PRINT else None
                            file_urls.append(file_url)

                            document_type = columns[0].text.strip()
                            document_description = columns[1].text.strip()
                            document_date = columns[3].text.strip()
                            document_name = f'date={document_date}&type={document_type}&desc={document_description}&uid={n_documents}'
                            len_limitation = len(document_name) - max_file_name_len
                            print(f'    Doc {n_documents} len_limitation: {len_limitation}') if len_limitation > -5 else None
                            if len_limitation > 0:
                                document_description = document_description[:-len_limitation]
                                document_name = f'date={document_date}&type={document_type}&desc={document_description}&uid={n_documents}'
                            print(f'    Document {n_documents}: {document_name}') if PRINT else None

                            document_name = replace_invalid_characters(document_name)
                            # print('new: ', document_name) if PRINT else None
                            document_names.append(f'{self.data_upload_path}{folder_name}/{document_name}')
                        except NoSuchElementException:
                            print('file url not exist') if PRINT else None
                    # item = self.create_item(driver, folder_name, file_urls, document_names)
                    # yield item
            # --- --- --- Comments / Representations (csv + doc) --- --- --- Save comments as .csv + extract file_urls (if have) for downloading comment docs.
            elif tab_name == 'representations':  # e.g. auth=3, year=[24]
                comment_list = tab.find_element(By.XPATH, './div/div[2]/table/tbody')
                comment_items = comment_list.find_elements(By.XPATH, './tr')
                n_comments = len(comment_items)
                app_df.at['other_fields.n_comments'] = n_comments
                print(f'\n{tab_index+1} Representation Tab: {n_comments} items.')
                if n_comments > 0:
                    # save representations / comments.
                    comment_columns = tab.find_elements(By.XPATH, './div/div[2]/table/thead/tr/th')
                    assert len(comment_columns) == 4
                    # test for further re-organization.
                    #self.scrape_for_csv(csv_name='representations', table_columns=comment_columns, table_items=comment_items, folder_name=folder_name, path='td')
                    scrape_for_csv(csv_name='representations', table_columns=comment_columns, table_items=comment_items, data_storage_path=self.data_storage_path, folder_name=folder_name, path='td')
                    # download comment files (if have).
                    comment_files = comment_list.find_elements(By.XPATH, './tr/td[4]')
                    for comment_index, comment_file in enumerate(comment_files):
                        try:
                            file_url = comment_file.find_element(By.XPATH, './a').get_attribute('href')
                            file_urls.append(file_url)
                            document_names.append(f'{self.data_upload_path}{folder_name}/representation_doc{comment_index + 1}')
                            print(f'    - - - Representation {comment_index + 1}: Exists - - -') if PRINT else None
                        except NoSuchElementException:
                            print(f'    - - - Representation {comment_index + 1}: N/A - - -') if PRINT else None
            # --- --- --- Make a Comment / Representation (null) --- --- ---
            elif tab_name == 'make_a_representation':
                print(f'\n{tab_index+1}. Make a representation.')
            # --- --- --- Appeals (csv + doc) --- --- --- Save appeals as .csv + extract file_urls (if have) for downloading appeal docs.
            elif tab_name == 'appeals':  # e.g. auth=3, year=[7]
                # [Appeal Reference, Address, Ward, Community, Appeal Details, Decision Document, Non Decision Supporting Documents]
                appeal_list = tab.find_element(By.XPATH, './div/div[2]/table/tbody')
                appeal_items = appeal_list.find_elements(By.XPATH, './tr')
                n_appeals = len(appeal_items)
                print(f'\n{tab_index+1} Appeals Tab: {n_appeals} items.')
                if n_appeals > 0:
                    appeal_columns = tab.find_elements(By.XPATH, './div/div[2]/table/thead/tr/th')
                    # test for further re-organization.
                    #self.scrape_for_csv(csv_name='appeals', table_columns=appeal_columns, table_items=appeal_items, folder_name=folder_name, path='td')
                    scrape_for_csv(csv_name='appeals', table_columns=appeal_columns, table_items=appeal_items, data_storage_path=self.data_storage_path, folder_name=folder_name, path='td')
                    # download appeal files (if have).
                    decision_file_column_id = len(appeal_columns) - 1
                    assert appeal_columns[decision_file_column_id - 1].text.strip() == 'Decision Document'
                    decision_files = appeal_list.find_elements(By.XPATH, f'./tr/td[{decision_file_column_id}]')
                    support_files = appeal_list.find_elements(By.XPATH, f'./tr/td[{decision_file_column_id + 1}]')
                    for appeal_index in range(n_appeals):
                        try:
                            decision_file_url = decision_files[appeal_index].find_element(By.XPATH, './a').get_attribute('href')
                            file_urls.append(decision_file_url)
                            document_names.append(f'{self.data_upload_path}{folder_name}/appeal_decision_doc{appeal_index + 1}')
                            print(f'    - - - Appeal decision doc {appeal_index + 1}: Exists - - -') if PRINT else None
                        except NoSuchElementException:
                            print(f'    - - - Appeal decision doc {appeal_index + 1}: N/A - - -') if PRINT else None
                        try:
                            support_file_url = support_files[appeal_index].find_element(By.XPATH, './a').get_attribute('href')
                            file_urls.append(support_file_url)
                            document_names.append(f'{self.data_upload_path}{folder_name}/appeal_support_doc{appeal_index + 1}')
                            print(f'    - - - Appeal support doc {appeal_index + 1}: Exists - - -') if PRINT else None
                        except NoSuchElementException:
                            print(f'    - - - Appeal support doc {appeal_index + 1}: N/A - - -') if PRINT else None
            # --- --- --- Unknown Tabs --- --- ---
            else:
                print(f'\n{tab_index+1}. Unknown Tab: {tab_name}.')
                assert 1 == 0
        if len(file_urls) > 0:
            item = self.create_item(driver, folder_name, file_urls, document_names)
            yield item
        self.ending(app_df)
