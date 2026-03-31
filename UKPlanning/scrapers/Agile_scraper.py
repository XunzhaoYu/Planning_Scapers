import os, time, random, re
import pandas as pd

from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from tensorflow.python.framework.test_ops import n_polymorphic_restrict_in

from configs.settings import PRINT
from general.base_scraper import Base_Scraper
from general.document_utils import replace_invalid_characters, get_documents, get_NEC_or_Northgate_documents
from general.items import DownloadFilesItem
from general.utils import unique_columns, scrape_data_items, scrape_for_csv, scrape_multi_tables_for_csv  # test for further re-organization.


class Agile_Scraper(Base_Scraper):
    name = 'Agile_Scraper'
    """
    1.auth_id = 61, CannockChase: https://planning.agileapplications.co.uk/cannock/application-details/7007
    2.-auth_id = 139(137), Exmoor: https://planning.agileapplications.co.uk/exmoor/application-details/2552
    3.auth_id = 145(143), Flintshire: https://planning.agileapplications.co.uk/flintshire/application-details/28244
    4.auth_id = 202(200), LakeDistrict: https://planning.agileapplications.co.uk/ldnpa/application-details/27229 (many apps are unavailable)
    5.auth_id = 229(227), Middlesbrough: https://planning.agileapplications.co.uk/middlesbrough/application-details/1781
    6.-auth_id = 236(234), MoleValley: x
    7.auth_id = 244(242), NewForestPark: https://planning.agileapplications.co.uk/nfnpa/application-details/44335
    8.auth_id = 275(272), OldOakParkRoyal: https://planning.agileapplications.co.uk/opdc/application-details/8807
    9.auth_id = 281(278), Pembrokeshire: https://planning.agileapplications.co.uk/pembrokeshire/application-details/24026
    10.auth_id = 291(288), Redbridge: https://planning.agileapplications.co.uk/redbridge/application-details/110591
    11.auth_id = 304(301), Rugby: https://planning.agileapplications.co.uk/rugby/application-details/2777
    12.auth_id = 322(319), Slough: https://planning.agileapplications.co.uk/slough/application-details/11430
    13.auth_id = 346(343), Staffordshire: https://planning.agileapplications.co.uk/staffordshire/application-details/25518
    14.auth_id = 377(373), Tonbridge:   url error since 2003. solved with url_preprocess_Tonbridge and parse_Tonbridge_search_page_Agile. 
                                        https://planning.agileapplications.co.uk/tmbc/application-details/153514
    15.auth_id = 427(423), YorkshireDales:  url error. see parse_YorkshireDales_search_page_Agile for details.
                                            https://planning.agileapplications.co.uk/yorkshiredales/application-details/41484
    """

    # use pipelines_extension to obtain file extensions.
    # custom_settings = {'ITEM_PIPELINES': {'UKPlanning.pipelines.pipelines_extension.DownloadFilesPipeline': 1, }}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All sub_classes of Base_Scraper should define their self.parse_func(s) in __init__
        if self.auth in ['Tonbridge']:
            self.url_check = True
            self.url_preprocess = self.url_preprocess_Tonbridge
            self.parse_func = self.parse_Tonbridge_search_page_Agile
            print('self.parse_Tonbridge_search_page_Agile')
        elif self.auth in ['YorkshireDales']:
            self.parse_func = self.parse_YorkshireDales_fix_page_Agile
        else:
            self.parse_func = self.parse_data_item_Agile
            print('self.parse_data_item_Agile')

    details_dict ={'Application reference number': 'uid', # Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Redbridge, Rugby, Slough, Staffordshire, Tonbridge, YorkshireDales
                   'LA Reference': 'other_fields.LA_reference', # Flintshire
                   'Application type': 'other_fields.application_type', # Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Redbridge, Rugby, Slough, Staffordshire, Tonbridge, YorkshireDales
                   'Proposal description': 'description', # Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Rugby, Slough, Staffordshire, Tonbridge, YorkshireDales
                   'Full proposal description': 'description', # Redbridge
                   'Location': 'address', # Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Rugby, Slough, Staffordshire, Tonbridge, YorkshireDales
                   'Town or communty council': 'other_fields.parish', # Pembrokeshire
                   'Ward': 'other_fields.ward_name', # CannockChase, Flintshire, Middlesbrough, Pembrokeshire, OldOakParkRoyal, Redbridge, Rugby, Slough, Tonbridge
                   'District': 'other_fields.distric',  # Staffordshire
                   'Parish': 'other_fields.parish',  # CannockChase, LakeDistrict, Middlesbrough, NewForestPark, NewForestPark, Rugby, Slough, Staffordshire, Tonbridge, YorkshireDales
                   'Area': 'other_fields.area', # Flintshire, Slough, YorkshireDales
                   'Status': 'other_fields.status', # Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Redbridge, Rugby, Slough, Staffordshire, YorkshireDales
                   'Status description': 'other_fields.status_description', # Flintshire, Middlesbrough, NewForestPark, OldOakParkRoyal, Redbridge, Rugby, Slough, Staffordshire, YorkshireDales
                   'UPRN': 'other_fields.uprn', # Rugby
                   'Eircode': 'other_fields.eircode', # Rugby

                   'Registration date': 'other_fields.date_validated', # CannockChase, Flintshire, LakeDistrict, Middlesbrough, Slough
                   'Registered date': 'other_fields.date_validated',  # CannockChase, Redbridge, YorkshireDales
                   'Validated date': 'other_fields.date_validated', # Pembrokeshire
                   'Valid date':'other_fields.date_validated',  # Rugby
                   'Date valid': 'other_fields.date_validated', # Staffordshire
                   'Target date': 'other_fields.target_decision_date', # Staffordshire
                   'Revised target date': 'other_fields.revised_target_decision_date',  # Staffordshire
                   'Publicity start date': 'other_fields.publicity_start_date',  # Redbridge
                   'Publicity end date': 'other_fields.publicity_end_date',  # Redbridge
                   'Target Determination date': 'other_fields.determination_date', # Flintshire
                   'Application target date': 'other_fields.target_decision_date', # Redbridge, Tonbridge
                   'Level of Decision': 'other_fields.expected_decision_level', # Flintshire
                   'Extension of time date': 'other_fields.extension_of_time_date', # Flintshire, Middlesbrough, Pembrokeshire, OldOakParkRoyal, Redbridge, Rugby, Slough, Tonbridge, YorkshireDales
                   'Committee agenda item': 'other_fields.committee_agenda_item', # Rugby
                   'Committee Date': 'other_fields.meeting_date', # Rugby
                   'Decision level': 'other_fields.expected_decision_level',
                   'Decision': 'other_fields.decision', # Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Redbridge, Rugby, Slough, Staffordshire, Tonbridge, YorkshireDales
                   'Decision date': 'other_fields.decision_issued_date', # Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Redbridge, Rugby, Slough, Staffordshire, Tonbridge, YorkshireDales
                   'Decision Due Date': 'other_fields.decision_due_date', # YorkshireDales
                   'Decision expiry date': 'other_fields.decision_expiry_date', # Flintshire, Middlesbrough, OldOakParkRoyal, Rugby, Slough, YorkshireDales
                   'Dispatch date': 'other_fields.dispatch_date', # Staffordshire

                   'Appeal type': 'other_fields.appeal_type', # Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Redbridge, Rugby, Slough, Staffordshire, Tonbridge, YorkshireDales
                   'Appeal lodged date': 'other_fields.appeal_lodged_date', # Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Pembrokeshire, Redbridge, Rugby, Slough, Staffordshire, Tonbridge, YorkshireDales
                   'Appeal decision': 'other_fields.appeal_result', # Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Redbridge, Rugby, Slough, Staffordshire, Tonbridge, YorkshireDales
                   'Appeal decision date': 'other_fields.appeal_decision_date', # Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Redbridge, Rugby, Slough, Staffordshire, Tonbridge, YorkshireDales

                   'Agent name/Company name': 'other_fields.agent_name', # Pembrokeshire
                   'Applicants name': 'other_fields.applicant_name', # Flintshire
                   'Applicant’s name': 'other_fields.applicant_name', # Staffordshire
                   'Agent name (company)': 'other_fields.agent_name',  # CannockChase, Flintshire, NewForestPark, NewForestPark, OldOakParkRoyal, Rugby, Slough, YorkshireDales
                   'Agent’s name': 'other_fields.agent_name', # Staffordshire
                   'Officer name': 'other_fields.case_officer', # Flintshire, LakeDistrict, Middlesbrough, NewForestPark, Rugby, Slough, Staffordshire, Tonbridge, YorkshireDales
                   'Applicant surname/Company name': 'other_fields.applicant_name',
                   'Easting':  'other_fields.easting', # Flintshire, Rugby
                   'Northing': 'other_fields.northing', # Flintshire, Rugby
                   'Final date for third party observations/submissions': 'other_fields.final_date_for_third_party', # Flintshire*

                   # conditions:
                   #'Decision': 'other_fields.decision',
                   #'Decision date': 'other_fields.decision_issued_date',

                   # dates:
                   #'Registration date': 'other_fields.date_validated',  # duplicated: CannockChase, Flintshire, LakeDistrict, Middlesbrough, Slough, YorkshireDales
                   #'Validated date': 'other_fields.date_validated', # duplicated: Pembrokeshire
                   #'Valid date': 'other_fields.date_validated' # duplicated: Rugby
                   #'Date valid': 'other_fields.date_validated' # duplicated: Staffordshire
                   'Validation date': 'other_fields.date_validated', # NewForestPark
                   #'Target date': 'other_fields.target_decision_date',  # duplicated: Staffordshire
                   #'Revised target date': 'other_fields.revised_target_decision_date',  # duplicated: Staffordshire
                   #'Decision date': 'other_fields.decision_issued_date', # duplicated: CannockChase, Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Pembrokeshire, Rugby, Slough, Staffordshire, YorkshireDales
                   'Public comments by': 'other_fields.public_consultation_end_date',  # Staffordshire
                   'Consultee comments by': 'other_fields.consultation_end_date', # Staffordshire
                   'Consultation expiry': 'other_fields.consultation_end_date', # CannockChase, Flintshire, Middlesbrough, NewForestPark, OldOakParkRoyal.
                   'Consultation expiry date': 'other_fields.consultation_end_date', #  CannockChase*, Flintshire, Middlesbrough, OldOakParkRoyal, Pembrokeshire, Rugby, Slough
                   'Received date': 'other_fields.date_received', # CannockChase, Flintshire, LakeDistrict, Middlesbrough, Slough, Tonbridge
                   'Site notice date': 'other_fields.site_notice_start_date', #  CannockChase, Flintshire, Middlesbrough, OldOakParkRoyal, Rugby, Slough, Staffordshire
                   'Site Notice End': 'other_fields.site_notice_end_date',  # Redbridge, YorkshireDales
                   'Newspapers': 'other_fields.newspapers', #  CannockChase, Flintshire, OldOakParkRoyal, Slough
                   'Press notice start date': 'other_fields.press_notice_start_date', # CannockChase, Flintshire, Middlesbrough, OldOakParkRoyal, Slough
                   'Press notice date': 'other_fields.press_notice_start_date', # Staffordshire
                   'Press notice end date': 'other_fields.press_notice_end_date', # Pembrokeshire
                   'Press notice expiry date': 'other_fields.press_notice_end_date', #  YorkshireDales
                   'Press Notice reason': 'other_fields.press_notice_reason', # Rugby
                   'Target Decision Due Date': 'other_fields.target_decision_date', # Slough
                   #'Dispatch date': 'other_fields.dispatch_date', # duplicated: Staffordshire
                   #'Appeal lodged date': 'other_fields.appeal_lodged_date', # duplicated: CannockChas, Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Pembrokeshire, Rugby, Slough, Staffordshire, YorkshireDales
                   #'Appeal decision date': 'other_fields.appeal_decision_date', # duplicated: CannockChas, Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Pembrokeshire, Rugby, Slough, Staffordshire, YorkshireDales
                   }

    def scrape_data_items_from_AngularJS(self, app_df, item_list):
        contact_value = None
        for item in item_list:
            item_name = item.find_element(By.XPATH, './label').text.strip()
            #""" # ***
            if item_name == '':
                print(f'debug - item name: {item_name}')
                item_name = item.find_element(By.XPATH, './label').get_attribute('innerText').strip()
                print(f'new item name: {item_name}')
            #"""
            if item_name in ['Officer telephone']:  # YorkshireDales
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

    def url_preprocess_Tonbridge(self, url):
        if url.startswith('https://planning.agileapplications.co.uk/tmbc'):
            return url
        else:
            return 'https://planning.agileapplications.co.uk/tmbc/search-applications/'

    def parse_Tonbridge_search_page_Agile(self, response):
        app_df = response.meta['app_df']
        # url is correct, go to scraper.
        if app_df.at['url'].startswith('https://planning.agileapplications.co.uk/tmbc'):
            # scrape application directly.
            yield from self.parse_data_item_Agile(response)
        else: # url is replaced by the search page, go to search_by_appID.
            yield from self.search_by_appID_Agile(response)

    # A module to search applications using their app_id.
    def search_by_appID_Agile(self, response):
        driver = response.request.meta['driver']
        app_df = response.meta['app_df']
        #current_tab = driver.current_window_handle
        url = response.request.url
        print(f'search page url: {url}') if PRINT else None

        try:
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="header"]/sas-cookie-consent/section/section/div[1]/button[1]'))).click()
            print('Click: Accept.')
        except TimeoutException:
            print('No Cookie button.')
        # use app_id to search and view the application page.
        app_id = app_df.at['uid']
        input_reference = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//input[@name="reference"]')))
        input_reference.click()
        input_reference.send_keys(app_id)
        # click 'search' button.
        driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')  # scroll down to the bottom of this page.
        for tries in range(3):  # Sometimes there has no search result even if the app is available, so we try 3 times.
            time.sleep(random.uniform(1., 1.5))
            driver.find_element(By.ID, 'btnSearch').click()
            # click 'view' button.
            try:
                view_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//table[@name='results']/tbody/tr")))
                time.sleep(random.uniform(.5, 1.))
                view_button.click()
                break
            except TimeoutException:
                print(f'Timeout error - search result: Application {app_id} is not found.')
                if tries == 2:
                    return
        time.sleep(random.uniform(4., 5.))

        app_df.at['url'] = driver.current_url
        print(f'correct url: {driver.current_url}')

        # scrape application
        yield from self.parse_data_item_Agile(response)

    def parse_YorkshireDales_fix_page_Agile(self, response):
        app_df = response.meta['app_df']
        new_url = re.sub(r'yorkshiredale(?![s])', 'yorkshiredales', app_df.at['url'])
        app_df.at['url'] = new_url
        print(f'fixed url: {new_url}')
        yield SeleniumRequest(url=new_url, callback=self.parse_data_item_Agile, meta={'app_df': app_df}, dont_filter=True)

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
            note = response.xpath('//*[@id="page"]/div/div/div/h1/text()').get()
            print('note: ', note)
            return

        def expand_list(table):
            group_rows = table.find_elements(By.CSS_SELECTOR, "tr.ng-table-group")
            for row in group_rows:
                button = row.find_element(By.XPATH, './td/a')
                if 'right' in button.find_element(By.XPATH, './span[1]').get_attribute('class'):
                    button.click()
                    time.sleep(2)
                    assert 'down' in button.find_element(By.XPATH, './span[1]').get_attribute('class')

        def expand_list2(table, n_comments):
            # identify the list status
            n_rows = len(table.find_elements(By.XPATH, './tbody/tr'))
            if n_rows < n_comments:  # lists are not expanded. # we assume all lists share the same status.
                for list_index in range(n_rows, 0, -1):
                    button = table.find_element(By.XPATH, f'./tbody/tr[{list_index}]/td/a')
                    if 'right' in button.find_element(By.XPATH, './span[1]').get_attribute('class'):
                        button.click()
                        time.sleep(2)
                        assert 'down' in button.find_element(By.XPATH, './span[1]').get_attribute('class')

        tab_list = tab_panel.find_elements(By.XPATH, './div')
        for tab_index, tab in enumerate(tab_list):
            tab_name = tab.find_element(By.XPATH, './div/h4/a/span').text.strip()
            # check if the panel is opened, otherwise, data in this panel is not accessible.
            if 'panel-open' not in tab.get_attribute('class'):
                print(f'<panel-open> not in tab: {tab_name}')
                tab.find_element(By.XPATH, './div/h4/a').click()
                time.sleep(2)
            # --- --- --- Summary (data + optional: csv) --- --- ---
            if 'summary' in tab_name.lower():
                # summaryTab = tab.div[2]/div/summary/div
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
            # CannockChase, Flintshire(0), Middlesbrough(0), NewForestPark(0), OldOakParkRoyal, Rugby, Slough, YorkshireDales
            # consultee, neighbour, interested party
            # see https://planning.agileapplications.co.uk/opdc/application-details/8993
            elif 'consultation' in tab_name.lower():
                n_comments = int(re.findall(r'\(\s*(\d+)\s*\)', tab_name)[0])
                print(f'\n{tab_index + 1}. {tab_name} Tab.')
                app_df.at['other_fields.n_comments_consultee_total_consulted'] = 0
                app_df.at['other_fields.n_comments_public_total_consulted'] = 0
                if n_comments > 0:
                    consultation_table = driver.find_element(By.XPATH, "//table[@name='consultations']")
                    # identify the list status
                    expand_list(consultation_table)
                    items = consultation_table.find_elements(By.XPATH, './tbody/tr')

                    column_names = [column.get_attribute('data-title').strip() for column in items[1].find_elements(By.XPATH, './td')]
                    column_names = unique_columns(column_names)
                    n_columns = len(column_names)

                    # initialize a csv
                    csv_name = items[0].find_element(By.XPATH, './td/a/strong').get_attribute('innerText').split('(')[0].lower()
                    content_dict = {}
                    n_content = 0
                    for column_index in range(n_columns):
                        # content_dict[column_names[column_index]] = [table_item.find_element(By.XPATH, f'./td[{column_index + 1}]/span').get_attribute('innerText').strip() for table_item in items[1:]]
                        content_dict[column_names[column_index]] = []
                    for item in items[1:]:
                        class_attr = item.get_attribute("class")
                        if "ng-table-group" in class_attr:
                            content_df = pd.DataFrame(content_dict)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/{csv_name}.csv", index=False)
                            if csv_name == 'consultee':
                                app_df.at['other_fields.n_comments_consultee_total_consulted'] = n_content
                            else:  # if csv_name == 'neighbour':
                                app_df.at['other_fields.n_comments_public_total_consulted'] += n_content
                            print(f'    {csv_name}: {n_content} items.')
                            # initialize for the next csv file:
                            csv_name = item.find_element(By.XPATH, './td/a/strong').get_attribute('innerText').split('(')[0].lower()
                            assert csv_name in ['consultee', 'neighbour', 'interested party']  # test
                            content_dict = {}
                            n_content = 0
                            for column_index in range(n_columns):
                                content_dict[column_names[column_index]] = []
                        else:
                            for column_index in range(n_columns):
                                content_dict[column_names[column_index]].append(item.find_element(By.XPATH, f'./td[{column_index + 1}]/span').get_attribute('innerText').strip())
                            n_content += 1  # consultation table does not have multi-content in each row.  # int(content_dict[column_names[-1]][-1])
                        """
                        try:  # write data to csv file:
                            for column_index in range(n_columns):
                                content_dict[column_names[column_index]].append(item.find_element(By.XPATH, f'./td[{column_index + 1}]/span').get_attribute('innerText').strip())
                            n_content += 1 # consultation table does not have multi-content in each row.  # int(content_dict[column_names[-1]][-1])
                        except NoSuchElementException:  # save the current csv file:
                            content_df = pd.DataFrame(content_dict)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/{csv_name}.csv", index=False)
                            if csv_name == 'consultee':
                                app_df.at['other_fields.n_comments_consultee_total_consulted'] = n_content
                            else:  # if csv_name == 'neighbour':
                                app_df.at['other_fields.n_comments_public_total_consulted'] += n_content
                            print(f'    {csv_name}: {n_content} items.')
                            # initialize for the next csv file:
                            csv_name = item.find_element(By.XPATH, './td/a/strong').get_attribute('innerText').split('(')[0].lower()
                            assert csv_name in ['consultee', 'neighbour', 'interested party']  # test
                            content_dict = {}
                            n_content = 0
                            for column_index in range(n_columns):
                                content_dict[column_names[column_index]] = []
                        """

                    content_df = pd.DataFrame(content_dict)
                    content_df.to_csv(f'{self.data_storage_path}{folder_name}/{csv_name}.csv', index=False)
                    if csv_name == 'consultee':
                        app_df.at['other_fields.n_comments_consultee_total_consulted'] = n_content
                    else:  # if csv_name == 'neighbour':
                        app_df.at['other_fields.n_comments_public_total_consulted'] += n_content
                    # app_df.at[?] = n_content # test
                    print(f'    {csv_name}: {n_content} items.')

            # --- --- --- Responses (multiple multi-column csv) --- --- ---
            # CannockChase(0), Flintshire, Middlesbrough(0), NewForestPark, OldOakParkRoyal, Rugby, Slough, Tonbridge, YorkshireDales(0)
            # consultee, neighbour, interested party, applicant, parish
            elif 'responses' in tab_name.lower():
                n_responses = int(re.findall(r'\(\s*(\d+)\s*\)', tab_name)[0])
                print(f'\n{tab_index + 1}. {tab_name} Tab.')  # {n_responses} items.')
                app_df.at['other_fields.n_comments_consultee_responded'] = 0
                app_df.at['other_fields.n_comments_public_received'] = 0
                if n_responses > 0:
                    app_df.at['other_fields.n_comments'] = n_responses

                    responses_table = driver.find_element(By.XPATH, "//table[@name='responses']")
                    # identify the list status
                    expand_list(responses_table)
                    items = responses_table.find_elements(By.XPATH, './tbody/tr')

                    column_names = [column.get_attribute('data-title').strip() for column in items[1].find_elements(By.XPATH, './td')]
                    column_names = unique_columns(column_names)
                    n_columns = len(column_names)

                    # initialize a csv
                    csv_name = items[0].find_element(By.XPATH, './td/a/strong').get_attribute('innerText').split('(')[0].lower()
                    content_dict = {}
                    n_content = 0
                    for column_index in range(n_columns):
                        #content_dict[column_names[column_index]] = [table_item.find_element(By.XPATH, f'./td[{column_index + 1}]/span').get_attribute('innerText').strip() for table_item in items[1:]]
                        content_dict[column_names[column_index]] = []
                    for item in items[1:]:
                        class_attr = item.get_attribute("class")
                        if "ng-table-group" in class_attr:
                            content_df = pd.DataFrame(content_dict)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/responses-{csv_name}.csv", index=False)
                            if csv_name == 'consultee':
                                app_df.at['other_fields.n_comments_consultee_responded'] = n_content
                            else:  # if csv_name == 'neighbour':
                                app_df.at['other_fields.n_comments_public_received'] += n_content
                            print(f'    {csv_name}: {n_content} items.')
                            # initialize for the next csv file:
                            csv_name = item.find_element(By.XPATH, './td/a/strong').get_attribute('innerText').split('(')[0].lower()
                            assert csv_name in ['consultee', 'neighbour', 'interested party'] # test
                            content_dict = {}
                            n_content = 0
                            for column_index in range(n_columns):
                                content_dict[column_names[column_index]] = []
                        else:
                            for column_index in range(n_columns):
                                content_dict[column_names[column_index]].append(item.find_element(By.XPATH, f'./td[{column_index + 1}]/span').get_attribute('innerText').strip())
                            n_content += int(content_dict[column_names[-1]][-1])
                        """
                        try: # write data to csv file:
                            for column_index in range(n_columns):
                                content_dict[column_names[column_index]].append(item.find_element(By.XPATH, f'./td[{column_index+1}]/span').get_attribute('innerText').strip())
                            n_content += int(content_dict[column_names[-1]][-1])
                        except NoSuchElementException: # save the current csv file:
                            content_df = pd.DataFrame(content_dict)
                            content_df.to_csv(f"{self.data_storage_path}{folder_name}/responses-{csv_name}.csv", index=False)
                            if csv_name == 'consultee':
                                app_df.at['other_fields.n_comments_consultee_responded'] = n_content
                            else:  # if csv_name == 'neighbour':
                                app_df.at['other_fields.n_comments_public_received'] += n_content
                            print(f'    {csv_name}: {n_content} items.')
                            # initialize for the next csv file:
                            csv_name = item.find_element(By.XPATH, './td/a/strong').get_attribute('innerText').split('(')[0].lower()
                            assert csv_name in ['consultee', 'neighbour', 'interested party'] # test
                            content_dict = {}
                            n_content = 0
                            for column_index in range(n_columns):
                                content_dict[column_names[column_index]] = []
                        """

                    content_df = pd.DataFrame(content_dict)
                    content_df.to_csv(f'{self.data_storage_path}{folder_name}/responses-{csv_name}.csv', index=False)
                    if csv_name == 'consultee':
                        app_df.at['other_fields.n_comments_consultee_responded'] = n_content
                    else:  # if csv_name == 'neighbour':
                        app_df.at['other_fields.n_comments_public_received'] += n_content
                    # app_df.at[?] = n_content # test
                    print(f'    {csv_name}: {n_content} items.')

            # --- --- --- Constraints/Policies (multiple single-column csv) --- --- ---
            # CannockChase, Flintshire, Middlesbrough, NewForestPark, Rugby, Slough, Tonbridge
            # Policies only: Redbridge
            # constraint, policies, conservation areas, tree preservation orders, listed buildings.
            elif any(word in tab_name.lower() for word in ['constraint', 'policies']): # 'constraint' in tab_name.lower():
                # n_constraints = re.findall(r'\(\s*(\d+)\s*\)', tab_name)[0]
                item_table = driver.find_element(By.XPATH, '//*[@id="constraintsSection"]/section[2]/sas-table/div[2]/table/tbody')
                items = item_table.find_elements(By.XPATH, './tr')  #[1:]
                print(f'\n{tab_index + 1}. {tab_name} Tab.')  # {len(items)} items.')
                if len(items) == 0:
                    app_df.at['other_fields.n_constraints'] = 0
                else:
                    column_name = 'Description'
                    csv_name = items[0].find_element(By.XPATH, './td/a/strong').get_attribute('innerText').strip().lower()
                    table_content = []
                    for item in items[1:]:
                        try:
                            table_content.append(item.find_element(By.XPATH, f'./td/span').get_attribute('innerText').strip())
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
                # Pembrokeshire
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
                # CannockChase, Flintshire, LakeDistrict, Middlesbrough, NewForestPark
                except NoSuchElementException:
                    try:
                        n_documents = int(re.findall(r'\(\s*(\d+)\s*\)', tab_name)[0])
                    except IndexError:
                        n_documents = 0
                        assert 'no document' in driver.find_element(By.XPATH, '//*[@id="documentsTab"]/div/section[1]/div/span').get_attribute('innerText').strip()
                    print(f'\n{tab_index + 1}. Documents Tab: {n_documents} items, folder_name: {folder_name}')
                    app_df.at['other_fields.n_documents'] = n_documents
                    if n_documents > 0:
                        n_documents = 0
                        item_table = driver.find_element(By.XPATH, '//*[@id="documents"]/div[2]/table/tbody')
                        item_list = item_table.find_elements(By.XPATH, './tr')
                        document_type, document_description, document_date, file_urls, document_names  = None, None, None, [], []
                        if self.auth in ['YorkshireDales']:
                            LA_abbreviation = 'YD'
                        else:
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
                                if row_data['receivedDate']:
                                    document_name = f"date={row_data['receivedDate'][:10]}&type={row_data['mediaDescription']}&desc={row_data['description']}&uid={n_documents}.{document_extension}"
                                else:
                                    document_name = f"date=&type={row_data['mediaDescription']}&desc={row_data['description']}&uid={n_documents}.{document_extension}"
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

            # --- --- --- Conditions (data + one multi-column csv, multi-pages) --- --- ---
            # CannockChase, Flintshire, Middlesbrough, NewForestPark, Redbridge, Rugby, Slough
            elif 'condition' in tab_name.lower():
                n_conditions = int(re.findall(r'\(\s*(\d+)\s*\)', tab_name)[0])

                item_block = driver.find_element(By.XPATH, '//*[@id="conditionsTab"]/div') # //*[@id="conditionsTab"]/div
                class_attr = item_block.get_attribute('class')
                if 'ng-hide' in class_attr:
                    # //*[@id="conditionsTab"]/section[1]/div/span
                    print('\n', driver.find_element(By.XPATH, '//*[@id="conditionsTab"]/section[1]/div/span').get_attribute('innerText').strip())
                else:
                    item_list = item_block.find_elements(By.XPATH, './form/div')
                    # //*[@id="conditionsTab"]/div/form/div[1]/sas-input-text/div/div
                    print(f'\n{tab_index + 1}. {tab_name} Tab: {len(item_list)} items + {n_conditions} conditions.')
                    item_list = [item.find_element(By.XPATH, './*/div/div') for item in item_list]
                    app_df, _ = self.scrape_data_items_from_AngularJS(app_df, item_list)

                if n_conditions > 0:
                    csv_name = 'conditions'
                    content_dict = {}
                    if n_conditions > 10:
                        n_clicks = n_conditions//10
                        for i in range(n_clicks):
                            try:
                                show_more_results_button = driver.find_element(By.XPATH, '//*[@id="conditionsTab"]/section[2]/sas-table/div[2]/div[4]/div/div/a')
                                driver.execute_script("arguments[0].click();", show_more_results_button)
                                time.sleep(2)
                                print(f'{i+1}: click show more results.')
                            except NoSuchElementException:
                                break

                    #item_table = driver.find_element(By.XPATH, '//*[@id="conditionsTab"]/section[2]/sas-table/div[1]/table/tbody')
                    item_table = driver.find_element(By.XPATH, "//table[@name='conditions']/tbody")
                    items = item_table.find_elements(By.XPATH, './tr')
                    print(f'number of scraped conditions: {len(items)}')

                    column_names = [column.get_attribute('data-title').strip() for column in items[0].find_elements(By.XPATH, './td')]
                    column_names = unique_columns(column_names)
                    n_columns = len(column_names)

                    for column_index in range(n_columns):
                        content_dict[column_names[column_index]] = [table_item.find_element(By.XPATH, f'./td[{column_index+1}]/span').get_attribute('innerText').strip() for table_item in items]

                    content_df = pd.DataFrame(content_dict)
                    content_df.to_csv(f'{self.data_storage_path}{folder_name}/{csv_name}.csv', index=False)
                    # //*[@id="conditionsTab"]/section[2]/sas-table/div[1]/table/tbody/tr/td[1]/span

            # --- --- --- Dates (data) --- --- ---
            # CannockChase, Flintshire, LakeDistrict, Middlesbrough, NewForestPark, OldOakParkRoyal, Redbridge, Rugby, Slough, Staffordshire, YorkshireDales
            elif 'date' in tab_name.lower():
                item_list = driver.find_elements(By.XPATH, '//*[@id="datesTab"]/form/div')
                # //*[@id="datesTab"]/form/div[1]/div/sas-input-text/div/div
                print(f'\n{tab_index + 1}. {tab_name} Tab: {len(item_list)} items.')
                item_list = [item.find_element(By.XPATH, './div/*/div/div') for item in item_list]
                app_df, _ = self.scrape_data_items_from_AngularJS(app_df, item_list)

            # --- --- --- Further Info --- --- ---
            # Slough
            elif 'further info' in tab_name.lower():
                n_info = int(re.findall(r'\(\s*(\d+)\s*\)', tab_name)[0])
                print(f'\n{tab_index + 1}. {tab_name} Tab: {n_info} items.')
                if n_info > 0:
                    assert 1 == 0

            # --- --- --- Map --- --- ---
            # CannockChase, Flintshire, LakeDistrict, Staffordshire, Tonbridge
            elif 'map' in tab_name.lower():
                print(f'\n{tab_index + 1}. {tab_name} Tab.')

            else:
                print(f'\n{tab_index+1}. Unknown Tab: {tab_name}.')
                assert 1 == 0
        self.ending(app_df)