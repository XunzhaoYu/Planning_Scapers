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


class Idox_Scraper(Base_Scraper):
    name = 'Idox_Scraper'
    """
    auth_id = 31, Blackpool page:   https://idoxpa.blackpool.gov.uk/online-applications/applicationDetails.do?activeTab=summary&keyVal=_BLCKP_DCAPR_23433
    auth_id = 34, Bolton:   search: https://paplanning.bolton.gov.uk/online-applications/search.do?action=simple&searchType=Application
                            page:   https://paplanning.bolton.gov.uk/online-applications/applicationDetails.do?activeTab=summary&keyVal=ZZZPEGDEPM788
    auth_id = 
    """

    # use pipelines_extension to obtain file extensions.
    # custom_settings = {'ITEM_PIPELINES': {'UKPlanning.pipelines.pipelines_extension.DownloadFilesPipeline': 1, }}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All sub_classes of Base_Scraper should define their self.parse_func(s) in __init__
        if self.auth in ['Bolton']:
            self.url_check = True
            self.url_preprocess = self.url_preprocess_Bolton
        else:
            self.parse_func = self.parse_data_item_Idox

    # details_dict
    # self.app_df 81 - 19 + 8 = 70
    # Summary: 10 + 2*
    summary_dict = {'Reference': 'uid',  # Non-Empty
                    'Application Reference': 'uid',  # New Duplicate [Derby]
                    'Planning Portal Reference': 'other_fields.planning_portal_id',  # New [Derby]
                    'Alternative Reference': 'altid',
                    #
                    'Application Received': 'other_fields.date_received',
                    'Application Received Date': 'other_fields.date_received',  # New Duplicate [Chelmsford]
                    'Application Registered': 'other_fields.date_received',  # New Duplicate [Rhondda]
                    'Application Validated': 'other_fields.date_validated',
                    #
                    'Address': 'address',
                    'Location': 'address',  # Duplicate [Derby]
                    'Proposal': 'description',
                    'Status': 'other_fields.status',
                    'Decision': 'other_fields.decision',
                    'Decision Issued Date': 'other_fields.decision_issued_date',
                    'Appeal Status': 'other_fields.appeal_status',
                    'Appeal Decision': 'other_fields.appeal_result',
                    'Local Review Body Status': 'other_fields.local_review_body_status',  # New*
                    'Local Review Body Decision': 'other_fields.local_review_body_decision'  # New*
                    }
    # Further Information: 10 + 3 + 2*
    details_dict = {'Application Type': 'other_fields.application_type',
                    'Decision': 'other_fields.decision',  # Duplicated in summary
                    'Actual Decision Level': 'other_fields.actual_decision_level',  # New
                    'Expected Decision Level': 'other_fields.expected_decision_level',  # New
                    'Decision Level': 'other_fields.expected_decision_level',  # New Duplicate [Moray]
                    #
                    'Case Officer': 'other_fields.case_officer',
                    'Parish': 'other_fields.parish',
                    'Amenity Society': 'other_fields.amenity_society',  # New [Westminster]
                    'Ward': 'other_fields.ward_name',
                    'District Reference': 'other_fields.district',
                    'Applicant Name': 'other_fields.applicant_name',
                    'Applicant Address': 'other_fields.applicant_address',
                    'Agent Name': 'other_fields.agent_name',
                    'Agent Company Name': 'other_fields.agent_company',
                    'Agent Phone Number': 'other_fields.agent_phone',  # New*
                    'Agent Address': 'other_fields.agent_address',
                    'Environmental Assessment Requested': 'other_fields.environmental_assessment',  # New
                    'Environmental Assessment Required': 'other_fields.environmental_assessment',
                    # New Duplicate [Perth]
                    'Community Council': 'other_fields.community_council',  # New*
                    'Community': 'other_fields.community_council',  # New* Duplicate [BreconBeacons]
                    'Community/Town Council': 'other_fields.community_council',  # New* Duplicate [Caerphilly]
                    }
    # Important Datas: 14 + 4 + 1*
    dates_dict = {'Application Received Date': 'other_fields.date_received',  # Duplicated in summary
                  'Application Validated Date': 'other_fields.date_validated',  # Duplicated in summary
                  'Date Application Valid': 'other_fields.date_validated',
                  # Duplicated in summary [NewcastleUnderLyme]
                  'Application Valid Date': 'other_fields.date_validated',  # Duplicated in summary [Oadby]
                  'Valid Date': 'other_fields.date_validated',  # New Duplicated in summary [EastHampshire]
                  'Application Registered Date': 'other_fields.date_validated',
                  # New Duplicated in summary [Hammersmith]

                  'Expiry Date': 'other_fields.application_expires_date',
                  'Application Expiry Date': 'other_fields.application_expires_date',
                  # New Duplicate [MiltonKeynes]
                  'Application Expiry Deadline': 'other_fields.application_expires_date',  # New Duplicate [Sefton]

                  'Statutory Expiry Date': 'other_fields.statutory_expires_date',  # New []
                  #
                  'Expiry Date for Comment': 'other_fields.comment_expires_date',  # New
                  'Expiry Date for Comments': 'other_fields.comment_expires_date',  # New Duplicate [Moray]
                  'Last Date For Comments': 'other_fields.comment_expires_date',  # New Duplicate [Edinburgh]
                  'Last Date for Comments': 'other_fields.comment_expires_date',  # New Duplicate [Glasgow]
                  'Last date for public comments': 'other_fields.comment_expires_date',  # New Duplicate [Perth]
                  'Comments To Be Submitted By': 'other_fields.comment_expires_date',  # New Duplicate [Leeds]
                  'Closing Date for Comments': 'other_fields.comment_expires_date',  # New Duplicate [Hammersmith]
                  #
                  'Actual Committee Date': 'other_fields.meeting_date',
                  'Committee Date': 'other_fields.meeting_date',  # New Duplicate [Chelmsford]
                  'Actual Committee or Panel Date': 'other_fields.meeting_date',  # New Duplicate [Gedling]
                  'Date of Committee Meeting': 'other_fields.meeting_date',  # New Duplicate [IOW]
                  'Committee/Delegated List Date': 'other_fields.meeting_date',  # New Duplicate [WestLothian]
                  # Neighbour Consultation Date
                  'Latest Neighbour Consultation Date': 'other_fields.neighbour_consultation_start_date',
                  'Neighbours Last Notified': 'other_fields.neighbour_last_notified_date',
                  # New [NewcastleUnderLyme]
                  'Last Date for Neighbours Responses': 'other_fields.last_neighbour_responses_date',
                  # New [NewcastleUnderLyme]
                  # Neighbour Consultation Expiry
                  'Neighbour Consultation Expiry Date': 'other_fields.neighbour_consultation_end_date',
                  'Neighbour Comments should be submitted by Date': 'other_fields.neighbour_consultation_end_date',
                  # New Duplicate [Bedford]
                  'Neighbour Notification Expiry Date': 'other_fields.neighbour_notification_expiry_date',
                  # New [Sefton]
                  # Consultee Consultation Date
                  'Latest Statutory Consultee Consultation Date': 'other_fields.latest_consultee_consultation_date',
                  # New [Bedford]
                  'Statutory Consultee Consultation Expiry Date': 'other_fields.consultee_consultation_expiry_date',
                  # New [Bedford]
                  # Consultation Expiry
                  'Standard Consultation Date': 'other_fields.standard_consultation_start_date',
                  # *** changed from consultation_start to standard_cosultation_start
                  'Standard Consultation Expiry Date': 'other_fields.standard_consultation_end_date',
                  # *** changed from consultation_end to standard_cosultation_end

                  'Consultation Expiry Date': 'other_fields.consultation_end_date',  # New Duplicate [Chelmsford]
                  'Consultation Deadline': 'other_fields.consultation_end_date',  # New Duplicate [NorthSomerest]
                  'Consultation Period To End On': 'other_fields.consultation_end_date',  # New Duplicate [Torbay]
                  'Consultation End Date': 'other_fields.consultation_end_date',  # New Duplicate [TowerHamlets]

                  'Public Consultation Expiry Date': 'other_fields.public_consultation_end_date',
                  # New Duplicate [Oadby*** changed from consultation_end to public_xxx]
                  'Public Consultation End Date': 'other_fields.public_consultation_end_date',
                  # New Duplicate [IOW]
                  'Public Consultation Ends': 'other_fields.public_consultation_end_date',
                  # New Duplicate [Teignbridge]

                  'Overall Consultation Expiry Date': 'other_fields.overall_consultation_expires_date',  # New []
                  'Overall Date of Consultation Expiry': 'other_fields.overall_consultation_expires_date',
                  # New Duplicate []
                  # Advertisement
                  'Last Advertised In Press Date': 'other_fields.last_advertised_date',
                  'Advertised in Press Date': 'other_fields.last_advertised_date',  # New Duplicate [Glasgow]
                  'Latest Advertisement Expiry Date': 'other_fields.latest_advertisement_expiry_date',
                  'Advertisement Expiry Date': 'other_fields.latest_advertisement_expiry_date',
                  # New Duplicate [NorthHertfordshire]
                  # Site Notice
                  'Last Site Notice Posted Date': 'other_fields.site_notice_start_date',
                  'Latest Site Notice Expiry Date': 'other_fields.site_notice_end_date',
                  'Site Notice Expiry Date': 'other_fields.site_notice_end_date',
                  # New Duplicate [NorthHertfordshire]
                  # Target Date
                  'Internal Target Date': 'other_fields.target_decision_date',
                  'Target Date': 'other_fields.target_decision_date',  # New Duplicate [Bedford]
                  'Target Date for Decision': 'other_fields.target_decision_date',  # New Duplicate [Glasgow]
                  'Target Decision Date': 'other_fields.target_decision_date',  # New Duplicate [Stroud]

                  'Revised Target Date for Decision': 'other_fields.revised_target_decision_date',  # New [Glasgow]
                  'Revised Target Decision Date': 'other_fields.revised_target_decision_date',
                  # New Duplicate [Stroud]

                  'Agreed Extended Target Date': 'other_fields.agreed_extended_target_date',  # New [Teignbridge]
                  'Agreed Extended Date for Decision': 'other_fields.agreed_extended_decision_date',  # New [IOW]
                  # Decision Date
                  'Decision Made Date': 'other_fields.decision_date',
                  'Decision Date': 'other_fields.decision_date',  # Duplicated [Hammersmith]
                  'Decision Issued Date': 'other_fields.decision_issued_date',  # Duplicated in summary

                  'Decision Notice Date': 'other_fields.decision_notice_date',  # New [NewcastleUnderLyme]
                  'Statutory Decision Date': 'other_fields.statutory_decision_date',  # New [IOW]
                  'Earliest Decision Date': 'other_fields.earliest_decision_date',  # New [NewcastleUnderLyme]
                  'Agreed Expiry Date': 'other_fields.agreed_expires_date',  # New
                  'Permission Expiry Date': 'other_fields.permission_expires_date',

                  'Decision Printed Date': 'other_fields.decision_published_date',
                  'Decision Due Date': 'other_fields.decision_due_date',  # New [Chelmsford]
                  'Environmental Impact Assessment Received': 'other_fields.environmental_assessment_date',  # New
                  # Determination
                  'Determination Deadline': 'other_fields.determination_date',  # New
                  'Statutory Determination Deadline': 'other_fields.statutory_determination_deadline',  # New []
                  'Statutory Determination Date': 'other_fields.statutory_determination_deadline',
                  # New Duplicate [Oadby]
                  'Statutory Determination Deadline (Unless there is an Agreed extension date above)': 'other_fields.statutory_determination_deadline',
                  # New Duplicate [Bedford]
                  'Extended Determination Deadline': 'other_fields.extended_determination_deadline',
                  # New [NorthSomerest]
                  'Agreed Extension to Statutory Determination Deadline': 'other_fields.extended_determination_deadline',
                  # New Duplicate [Bedford]

                  'Temporary Permission Expiry Date': 'other_fields.temporary_permission_expires_date',  # New
                  'Local Review Body Decision Date': 'other_fields.local_review_body_decision_date'  # New*
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

    def url_preprocess_Bolton(self, url):
        if url.startswith('https://paplanning.bolton.gov.uk/online-applications/applicationDetails.do?'):
            self.parse_func = self.parse_data_item_Idox
            return url
        else:
            self.parse_func = self.search_by_appID_Idox
            return 'https://paplanning.bolton.gov.uk/online-applications/search.do?action=simple&searchType=Application'

    def search_by_appID_Idox(self, response):
        driver = response.request.meta['driver']
        app_df = response.meta['app_df']
        url = response.request.url
        print(f'search page url: {url}') if PRINT else None

        # use app_id to search and view the application page.
        app_id = app_df.at['uid']
        input_reference = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//input[@id="simpleSearchString"]')))
        input_reference.click()
        input_reference.send_keys(app_id)
        # click 'search' button.
        driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')  # scroll down to the bottom of this page.
        for tries in range(3):  # Sometimes there has no search result even if the app is available, so we try 3 times.
            time.sleep(random.uniform(1., 1.5))
            try:
                driver.find_element(By.XPATH, '//input[@type="submit"]').click()
                #search_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//button[@class="searchbutton"]')))
                #search_button.click()
            except NoSuchElementException:
                time.sleep(2)
        # will re-direct to the application pages automatically, just wait...
        while driver.current_url == url:
            time.sleep(random.uniform(4., 5.))
        """
        the url we get from search page is a temp url, not a factual url we can use in the future:    
        https://paplanning.bolton.gov.uk/online-applications/simpleSearchResults.do?action=firstPage
        """
        # get the factual url:
        summary_tab = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="subtab_summary"]')))
        url = summary_tab.get_attribute('href')
        app_df.at['url'] = response.urljoin(url)
        print(f"correct url: {app_df.at['url']}")

        # scrape application
        yield from self.parse_data_item_Idox(response)

    def parse_data_item_Idox(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta['driver']
        scraper_name = app_df.at['scraper_name']
        folder_name = self.setup_storage_path(app_df)
        max_file_name_len = self.max_folder_file_name_len - len(folder_name) - 5  # 5 chars for suffix/extension, such as .pdf
        print(f'parse_data_item_IdoxScraper, scraper name: {scraper_name}, max_file_name_len: {max_file_name_len}.')

        try:
            content = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="pa"]/div[@class="container"]/div[@class="content"]')))
        except TimeoutException:
            # Planning Application details not available.
            note = response.xpath('//*[@id="main-content"]/article/h1/text()').get()
            print('note: ', note)
            return

        tabs = content.find_elements(By.XPATH, "./ul[@class='tabs']/li")  # ”./ul/li"
        #sub_tabs = tabs[0].find_elements(By.XPATH, "./ul[@class='subtabs']/li")
        tab_container = content.find_element(By.XPATH, "./div[@class='tabcontainer']")   #  ”./div[3]“

        def scrape_data(app_df, items, item_values, dictionary):
            for item, value in zip(items, item_values):
                item_name = item.get_attribute('innerText').strip()
                data_name = dictionary[item_name]
                item_value = value.get_attribute('innerText').strip()
                try:
                    if pd.isnull(app_df.at[data_name]):
                        app_df.at[data_name] = item_value
                        print(f'    <{item_name}> scraped: {app_df.at[data_name]}') if PRINT else None
                    elif app_df.at[data_name].lower() == 'see source':
                        app_df.at[data_name] = item_value
                        print(f'    <{item_name}> source scraped: {app_df.at[data_name]}') if PRINT else None
                    else:
                        print(f"    <{item_name}> filled: {app_df.at[data_name]}") if PRINT else None
                except KeyError:
                    app_df[data_name] = item_value
                    print(f'    <{item_name}> scraped (new): {app_df.at[data_name]}') if PRINT else None
            return app_df

        # Details/Summary: tabs[0]/subtabs[0]
        items = tab_container.find_elements(By.XPATH, "./table[@id='simpleDetailsTable']/tbody/tr/th")
        item_values = tab_container.find_elements(By.XPATH, "./table[@id='simpleDetailsTable']/tbody/tr/td")
        print(f'\nDetails/Summary: {len(items)} items.')
        #app_df = scrape_data_items(app_df, items, item_values, self.summary_dict, PRINT)
        app_df = scrape_data(app_df, items, item_values, self.summary_dict)

        # Details/Further Information: tabs[0]/subtabs[1]
        #time.sleep(random.uniform(10, 15))
        #sub_tabs[1].find_element(By.XPATH, './a').click()
        driver.find_element(By.XPATH, '//*[@id="subtab_details"]').click()
        tbody = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table[@id='applicationDetails']/tbody")))
        items = tbody.find_elements(By.XPATH, './tr/th')
        item_values = tbody.find_elements(By.XPATH, './tr/td')
        print(f'\nDetails/Further Information: {len(items)} items.')
        #app_df = scrape_data_items(app_df, items, item_values, self.details_dict, PRINT)
        app_df = scrape_data(app_df, items, item_values, self.details_dict)

        # Details/Contacts: tabs[0]/subtabs[2]


        # Details/Important Dates: tabs[0]/subtabs[3]
        #time.sleep(random.uniform(10, 15))
        #sub_tabs[3].find_element(By.XPATH, './a').click()
        driver.find_element(By.XPATH, '//*[@id="subtab_dates"]').click()
        tbody = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table[@id='simpleDetailsTable']/tbody")))
        items = tbody.find_elements(By.XPATH, './tr/th')
        item_values = tbody.find_elements(By.XPATH, './tr/td')
        print(f'\nDetails/Important Dates: {len(items)} items.')
        # app_df = scrape_data_items(app_df, items, item_values, self.dates_dict, PRINT)
        app_df = scrape_data(app_df, items, item_values, self.dates_dict)

        time.sleep(1000)
        self.ending(app_df)