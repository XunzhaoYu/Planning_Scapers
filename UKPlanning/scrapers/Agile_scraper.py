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


class Agile_Scraper(Base_Scraper):
    name = 'Agile_Scraper'
    """
    1.auth_id = 61, CannockChase: https://planning.agileapplications.co.uk/cannock/application-details/7007
    2.auth_id = 139(137), Exmoor: https://planning.agileapplications.co.uk/exmoor/application-details/2552
    3.auth_id = 145(143), Flintshire: https://planning.agileapplications.co.uk/flintshire/application-details/28244
    4.auth_id = 202(200), LakeDistrict: https://planning.agileapplications.co.uk/ldnpa/application-details/27229
    5.auth_id = 229(227), Middlesbrough: https://planning.agileapplications.co.uk/middlesbrough/application-details/1781
    6.auth_id = 236(234), MoleValley: x
    7.auth_id = 244(242), NewForestPark: https://planning.agileapplications.co.uk/nfnpa/application-details/44335
    8.auth_id = 275(272), OldOakParkRoyal: https://planning.agileapplications.co.uk/opdc/application-details/8807
    9.auth_id = 281(278), Pembrokeshire: https://planning.agileapplications.co.uk/pembrokeshire/application-details/24026
    10.auth_id = 291(288), Redbridge: https://planning.agileapplications.co.uk/redbridge/application-details/110591
    11.auth_id = 304(301), Rugby: https://planning.agileapplications.co.uk/rugby/application-details/2777
    12.auth_id = 322(319), Slough: https://planning.agileapplications.co.uk/slough/application-details/11430
    13.auth_id = 346(343), Staffordshire: https://planning.agileapplications.co.uk/staffordshire/application-details/25518
    14.auth_id = 377(373), Tonbridge: https://planning.agileapplications.co.uk/tmbc/application-details/153514
    15.auth_id = 427(423), YorkshireDales: https://planning.agileapplications.co.uk/yorkshiredale/application-details/488
    """

    # use pipelines_extension to obtain file extensions.
    # custom_settings = {'ITEM_PIPELINES': {'UKPlanning.pipelines.pipelines_extension.DownloadFilesPipeline': 1, }}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All sub_classes of Base_Scraper should define their self.parse_func(s) in __init__
        self.parse_func = self.parse_data_item_Agile

    details_dict ={'Application reference number': 'uid',
                   'LA reference': '--- --- --- ---', # Flintshire
                   'Application type': 'other_fields.application_type',
                   'Proposal description': 'description',
                   'Location': 'address',
                   'Town or communty council': 'other_fields.parish', # Pembrokeshire
                   'Ward': 'other_fields.ward_name', # Flintshire, Pembrokeshire
                   'Parish': 'other_fields.parish',  # NewForestPark
                   'Area': 'other_fields.parish', # Flintshire
                   'Status': 'other_fields.status',
                   'Status description': '--- --- --- ---', #
                   'Validated date': 'other_fields.date_validated',
                   'Extension of time date': '--- --- --- ---', # Pembrokeshire
                   'Decision level': 'other_fields.expected_decision_level',
                   'Decision': 'other_fields.decision',
                   'Decision date': 'other_fields.decision_issued_date',
                   'Decision expiry date': '--- --- --- ---', #
                   'Appeal type': '--- --- --- ---', #
                   'Appeal lodged date': '--- --- --- ---', # Flintshire, Pembrokeshire
                   'Appeal decision': 'other_fields.appeal_result',
                   'Appeal decision date': 'other_fields.appeal_decision_date',
                   'Agent name/Company name': 'other_fields.agent_name', # Pembrokeshire
                   'Agent name (company)': 'other_fields.agent_name',  # NewForestPark
                   'Officer name': 'other_fields.case_officer',
                   'Applicant surname/Company name': 'other_fields.applicant_name',
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

    def parse_data_item_Agile(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta['driver']
        scraper_name = app_df.at['scraper_name']
        folder_name = self.setup_storage_path(app_df)
        max_file_name_len = self.max_folder_file_name_len - len(folder_name) - 5  # 5 chars for suffix/extension, such as .pdf
        print(f'parse_data_item_newScraper, scraper name: {scraper_name}, max_file_name_len: {max_file_name_len}.')

        try:
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="content"]/div/section')))
        except TimeoutException:
            # Planning Application details not available.
            note = response.xpath('//*[@id="main-content"]/article/h1/text()').get()
            print('note: ', note)
            return

        self.ending(app_df)