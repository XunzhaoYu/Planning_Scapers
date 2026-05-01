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


class CivicaJason_Scraper(Base_Scraper):
    name = 'CivicaJason_Scraper'
    """
    1/2/5(11 display:None) similar: <div/div/div class=civica-keyobject-basicdetails> + <div detail tab> + <div detail panel/div/div class=civica-keyobject-fulldetails> 
    4 minor different (no detail tab): <div/div/div class=civica-keyobject-basicdetails> + <div/div/div class=civica-keyobject-fulldetails>  
    3 major different (no basic detail): <div/div/div class=civica-keyobject-fulldetails>  
    6 completely different
    
    1.auth_id = 13, Ashfield: https://planning.ashfield.gov.uk/planning-applications/planning-application/?RefType=GFPlanning&KeyNo=194603
    2.auth_id = 99, Denbighshire: url error. https://planning.denbighshire.gov.uk/planning/planning-application?RefType=PBDC&KeyNo=11872
      cymraeg                       search page: https://planning.denbighshire.gov.uk/planning/ 
                                    app page: https://planning.denbighshire.gov.uk/planning/planning-application?RefType=PBDC&KeyNo=28889
    3.auth_id = 115(114), Eastbourne: url error. https://www.lewes-eastbourne.gov.uk/planning/application-summary?RefType=APPPlanCase&KeyText=190026
                                    search page: https://www.lewes-eastbourne.gov.uk/planning
                                    app page: https://www.lewes-eastbourne.gov.uk/article/2087/?RefType=APPPlanCase&KeyText=190026
    4.auth_id = 348(345), StAlbans: https://planningapplications.stalbans.gov.uk/planning/search-applications#VIEW?RefType=PBDC&KeyNo=109235
    5.auth_id = 393(389), Waverley: url error. http://planning360.waverley.gov.uk/planning/planning-application?RefType=GFPlanning&KeyNo=410184
                                    search page: https://planning360.waverley.gov.uk:4443/planning
                                    app page: https://planning360.waverley.gov.uk:4443/planning/search-applications?civica.query.FullTextSearch=WA%2F2020%2F0069%20#VIEW?RefType=GFPlanning&KeyNo=497728&KeyText=Subject
    6.auth_id = 421(417), Wrexham: url error. https://planning.wrexham.gov.uk/planning/planning-application?RefType=GFPlanning&KeyNo=69899
      cymraeg                       search page: https://register.wrexham.gov.uk/pr/s/register-view?c__r=Arcus_BE_Public_Register&language=en_GB
      quite different               app page: https://register.wrexham.gov.uk/pr/s/detail/a0lJ7000000TviIIAS?c__r=Arcus_BE_Public_Register&language=en_GB
    """

    # use pipelines_extension to obtain file extensions.
    custom_settings = {'ITEM_PIPELINES': {'UKPlanning.pipelines.pipelines_extension.DownloadFilesPipeline': 1, }}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All sub_classes of Base_Scraper should define their self.parse_func(s) in __init__
        self.parse_func = self.parse_data_item_CivicaJason

    details_dict = {'Reference Number': 'uid', # StAlbans
                    'Case No': 'uid', # Waverley

                    'Application Description': 'description',  # Eastbourne
                    'Proposal': 'description',  # Waverley
                    'Application Type': 'other_fields.application_type',  # Denbighshire, Eastbourne, StAlbans, Waverley

                    'Premises Address': 'address', # Ashfield, Denbighshire, Eastbourne
                    'Gaz Address': 'address', # Waverley
                    'Location': 'address', # StAlbans

                    'Applicant Name': 'other_fields.applicant_name', # Ashfield, StAlbans, Waverley
                    'Applicant': 'other_fields.applicant_name', # Denbighshire, Eastbourne
                    'Agent': 'other_fields.agent_name',  # All
                    'Case Officer': 'other_fields.case_officer',  # All

                    'Ward': 'ther_fields.ward_name',  # All
                    'Parish': 'other_fields.parish',  # All
                    'Decision Date': 'other_fields.decision_issued_date', # All
                    'Decision': 'other_fields.decision',  # All

                    'Received Date': 'other_fields.date_received', # StAlbans
                    'Date Valid': 'other_fields.date_validated', # Ashfield, Denbighshire, Eastbourne, StAlbans
                    'Date Advertised': 'other_fields.last_advertised_date', # StAlbans
                    #'Application Site Visit Date': '', # StAlbans
                    'Stage': 'other_fields.status', # Denbighshire, Eastbourne
                    'Committee Date': 'other_fields.meeting_date', # StAlbans
                    'Target Determination Date': 'other_fields.determination_date', # Eastbourne
                    'Decision Due By': 'other_fields.decision_due_date', # Ashfield, Denbighshire, Waverley
                    'Please Comment By': 'other_fields.comment_expires_date', # Denbighshire, Eastbourne, StAlbans
                    'Comments due by date': 'other_fields.comment_expires_date', # Waverley
                    'Expiry Date': 'other_fields.application_expires_date', # StAlbans
                    'Decision Level': 'other_fields.expected_decision_level', # StAlbans

                    #'Appeal Date': '', # Waverley
                    'Appeal Lodged Date': 'other_fields.appeal_lodged_date', # StAlbans
                    'Appeal Reference': 'other_fields.appeal_reference', # StAlbans
                    #'Appeal Method': '', # StAlbans
                    'Appeal Decision Date': 'other_fields.appeal_decision_date', # StAlbans
                    'Appeal Status': 'other_fields.appeal_status', # Denbighshire, Eastbourne, StAlbans, Waverley
                    }

    doc_url_dict = {'Ashfield': 'https://planning.ashfield.gov.uk/my-requests/document-viewer?DocNo=',
                    'Denbighshire': 'https://planning.denbighshire.gov.uk/my-requests/document-viewer?DocNo=',
                    # 'https://planning.denbighshire.gov.uk/w2webparts/Resource/Civica/Handler.ashx/Doc/pagestream?cd=inline&pdf=true&docno=',
                    'Eastbourne': 'https://www.lewes-eastbourne.gov.uk/2088/?DocNo=',
                    'StAlbans': 'https://planningapplications.stalbans.gov.uk/planning/search-applications#DOC?DocNo=',
                    # url: https://planning360.waverley.gov.uk:4443/planning/search-applications?civica.query.FullTextSearch=WA%2F2020%2F0069%20#VIEW?RefType=GFPlanning&KeyNo=497728&KeyText=Subject
                    'Waverley': 'https://planning360.waverley.gov.uk:4443/planning/search-applications?civica.query.FullTextSearch=WA%2F2020%2F0069%20#DOC?DocNo=',
                    'Wrexham': ''}

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

    def parse_data_item_CivicaJason(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta['driver']
        scraper_name = app_df.at['scraper_name']
        folder_name = self.setup_storage_path(app_df)
        max_file_name_len = self.max_folder_file_name_len - len(folder_name) - 5  # 5 chars for suffix/extension, such as .pdf
        print(f'parse_data_item_CivicaJason, scraper name: {scraper_name}, max_file_name_len: {max_file_name_len}.')

        try: # class = col-md-9 col-sm-9
            content = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//div[@role='tablist']/div/div")))
            """
            1 //*[@id="applicationviewer"]/div/div/div
            2 //*[@id="applicationviewer"]/div/div/div
            3 //*[@id="applicationSummary"]/div/div/div
            4 //*[@id="civica-planningsearchandview"]/div/div[2]/div/div/div/div
            5 //*[@id="civica-planningsearchandview"]/div/div[2]/div/div/div/div
            """
        except TimeoutException:
            # Planning Application details not available.
            note = response.xpath('//*[@id="main-content"]/article/h1/text()').get()
            print('note: ', note)
            return

        tab_list = content.find_elements(By.XPATH, "./div[@role='tab']")
        tab_panel_list = content.find_elements(By.XPATH, "./div[@role='tabpanel']")

        # tab_panel_list/div/div/div[@class='civicadetail']
        #
        item_list = content.find_elements(By.XPATH, "./div/div/div[@class='civica-keyobject-fulldetails']/div[@class='civicadetail']")
        print(f'\n1. Details Tab: {len(item_list)} items.')
        items = [item.find_element(By.XPATH, './div[1]') for item in item_list]
        item_values = [item.find_element(By.XPATH, './div[2]') for item in item_list]
        app_df = scrape_data_items(app_df, items, item_values, self.details_dict, PRINT)

        for tab_index, tab in enumerate(tab_list):
            tab_name = tab.find_element(By.XPATH, './div').text.strip()
            if 'false' in tab.get_attribute('aria-expanded'):
                print(f'panel {tab_name} expanded: false.')
                tab.click()
                time.sleep(2)
            # --- --- --- Details (data) --- --- ---
            if 'detail' in tab_name.lower():
                #item_list = tab_panel_list[tab_index].find_elements(By.XPATH, '')
                pass
            # --- --- --- Documents (doc) --- --- ---
            elif 'document' in tab_name.lower():
                def get_documents():
                    file_urls, document_names = [], []
                    try:
                        document_list = WebDriverWait(driver, 3).until(EC.visibility_of_element_located((By.CLASS_NAME, 'civica-doclist')))
                        document_items = document_list.find_elements(By.XPATH, './ul/li')
                        n_documents = len(document_items)
                    except NoSuchElementException:
                        n_documents = 0
                    app_df.at['other_fields.n_documents'] = n_documents
                    print(f'\n{tab_index+1}. Documents Tab: {n_documents} items.')
                    if n_documents > 0:
                        n_documents = 0
                        for document_item in document_items:
                            n_documents += 1
                            print(f'    - - - Document {n_documents} - - -') if PRINT else None
                            file_id = document_item.find_element(By.XPATH, './a').get_attribute('href').split('?DocNo=')[1]
                            file_url = f'{self.doc_url_dict[self.auth]}{file_id}'
                            print(f'    {file_url}') if PRINT else None
                            file_urls.append(file_url)

                            document_date = document_item.find_element(By.CLASS_NAME, 'civica-doclistdetailtext').text.strip()
                            document_description = document_item.find_element(By.CLASS_NAME, 'civica-doclisttitletext').text.strip()
                            document_name = f"date={document_date}&desc={document_description}&uid={n_documents}"  # .{item_extension}"

                            len_limitation = len(document_name) - max_file_name_len
                            print(f'    Doc {n_documents} len_limitation: {len_limitation}') if len_limitation > -5 else None
                            if len_limitation > 0:
                                document_description = document_description[:-len_limitation]
                                document_name = f'date={document_date}&desc={document_description}&uid={n_documents}'
                            print(f'    Document {n_documents}: {document_name}') if PRINT else None
                            document_name = replace_invalid_characters(document_name)
                            # print('new: ', document_name) if PRINT else None
                            document_names.append(f'{self.data_upload_path}{folder_name}/{document_name}')
                    return file_urls, document_names
                file_urls, document_names = get_documents()
                if len(file_urls) > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names)
                    yield item
            # --- --- --- Comments () --- --- ---
            elif 'comment' in tab_name.lower():
                n_objections_comments = tab_panel_list[tab_index].find_element(By.XPATH, ".//div[contains(@class, 'commentsobjections')]").get_attribute('innerText').strip()
                n_supporting_comments = tab_panel_list[tab_index].find_element(By.XPATH, ".//div[contains(@class, 'commentssupporting')]").get_attribute('innerText').strip()
                n_neither_comments = tab_panel_list[tab_index].find_element(By.XPATH, ".//div[contains(@class, 'commentsneither')]").get_attribute('innerText').strip()
                print(f'\n{tab_index+1}. Comments Tab: {n_objections_comments} objections, {n_supporting_comments} supporting, {n_neither_comments} neither.')
                app_df['other_fields.n_comments_public_objections'] = n_objections_comments
                app_df['other_fields.n_comments_public_supporting'] = n_supporting_comments
                app_df['other_fields.n_comments_public_received'] = n_objections_comments + n_supporting_comments + n_neither_comments
            else:
                print(f'\n{tab_index + 1}. Unknown Tab: {tab_name}.')
                assert 1 == 0
        self.ending(app_df)