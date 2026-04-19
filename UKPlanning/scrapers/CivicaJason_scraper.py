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
    # custom_settings = {'ITEM_PIPELINES': {'UKPlanning.pipelines.pipelines_extension.DownloadFilesPipeline': 1, }}

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
        tab_panel_list = content.find_elements(By.XPATH, "./div[@role='tab-panel']")

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
                pass
            # --- --- --- Comments () --- --- ---
            elif 'comment' in tab_name.lower():
                pass
            else:
                print(f'\n{tab_index + 1}. Unknown Tab: {tab_name}.')
                assert 1 == 0
        self.ending(app_df)