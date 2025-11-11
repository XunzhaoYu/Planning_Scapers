import os, time, random
import pandas as pd

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from configs.settings import PRINT
from general.base_scraper import Base_Scraper
from general.document_utils import replace_invalid_characters, get_documents
from general.items import DownloadFilesItem
from general.utils import unique_columns, scrape_data_items, scrape_for_csv, scrape_multi_tables_for_csv  # test for further re-organization.


# To do: Christchurch scraper.
class CCED_Scraper(Base_Scraper):
    name = 'CCED_Scraper'  # Similar to Atrium, but has different document system (FormRequest + payloads)
    """
    auth_id = 80, Christchurch: https://planning.christchurchandeastdorset.gov.uk/plandisp.aspx?recno=13511
    auth_id = 108(107), DorsetCouncil: https://planning.dorsetcouncil.gov.uk/plandisp.aspx?recno=72429
    """

    # use pipelines_form_extension to obtain file extensions.
    custom_settings = {'ITEM_PIPELINES': {'UKPlanning.pipelines.pipelines_form_extension.DownloadFilesPipeline': 1, }}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All sub_classes of Base_Scraper should define their self.parse_func(s) in __init__
        self.parse_func = self.parse_data_item_DorsetCouncil

    details_dict = {# Main Details
                    'Application No': 'uid',
                    'Type': 'other_fields.application_type',
                    'Case Officer': 'other_fields.case_officer',

                    'Committee / Delegated': 'other_fields.expected_decision_level',
                    'Status': 'other_fields.status',
                    'Committee Date': 'other_fields.meeting_date',
                    'Proposal': 'description',

                    'Valid Date': 'other_fields.date_validated',
                    'Decision': 'other_fields.decision',
                    'Issue Date': 'other_fields.decision_issued_date',
                    'Consultation End': 'other_fields.consultation_end_date',  # Christchurch only
                    'Neighbour Expiry': 'other_fields.neighbour_expiry',  # new: DorsetCouncil only.
                    'Authority': 'other_authority',  # new
                    # Location:
                    'Address': 'address',
                    'Easting': 'other_fields.easting',
                    'Northing': 'other_fields.northing',
                    'Ward': 'other_fields.ward_name',
                    'Parish': 'other_fields.parish',

                    'Ward members': 'Ward Members',  # A link.
                    # Appeals:
                    'Appeal_Number': 'other_fields.appeal_reference',
                    'Appeal_PI ref': 'other_fields.appeal_PI_reference',  # new
                    'Appeal_Method': 'other_fields.appeal_method',
                    'Appeal_Start date': 'other_fields.appeal_start_date',
                    'Appeal_Comment to PINS by': 'other_fields.appeal_comment_to_PINS_by',  # new
                    'Appeal_Inquiry Date': 'other_fields.appeal_inquiry_date',  # new
                    'Appeal_Venue': 'other_fields.appeal_venue',  # new
                    'Appeal_Decision': 'other_fields.appeal_result',
                    'Appeal_Issue Date': 'other_fields.appeal_decision_date'
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
                print(f'    <{item_name}> scraped: {app_df.at[data_name]}') if PRINT else None
            # New
            except KeyError:
                app_df[data_name] = item_value
                print(f'    <{item_name}> scraped (new): {app_df.at[data_name]}') if PRINT else None
        return app_df 

    def scrape_for_csv(self, csv_name, table_columns, table_items, folder_name, path='td'):
        content_dict = {}
        column_names = [column.text.strip() for column in table_columns]
        column_names = unique_columns(column_names)
        n_columns = len(column_names)

        for column_index in range(n_columns):
            content_dict[column_names[column_index]] = [table_item.find_element(By.XPATH, f'./{path}[{column_index+1}]').text.strip() for table_item in table_items]

        content_df = pd.DataFrame(content_dict)
        content_df.to_csv(f"{self.data_storage_path}{folder_name}/{csv_name}.csv", index=False) 

    def scrape_multi_tables_for_csv(self, csv_names, tables, folder_name, table_path='tbody/tr', column_path='th', item_path='td'):
        n_table_items = []
        for table_index, table in enumerate(tables):
            # table_name = table_names[table_index].text.strip().lower()
            table_rows = table.find_elements(By.XPATH, f'./{table_path}')
            table_columns = table_rows[0].find_elements(By.XPATH, f'./{column_path}')
            if len(table_columns) > 0:
                table_items = table_rows[1:]
                self.scrape_for_csv(csv_names[table_index], table_columns, table_items, folder_name, path=item_path)
                print(f'{csv_names[table_index]}, {len(table_items)} items') if PRINT else None
                n_table_items.append(len(table_items))
            else:
                table_item = table_rows[0].find_element(By.XPATH, f'./{item_path}').text.strip()
                print(f"{csv_names[table_index]} <NULL>: {table_item}") if PRINT else None
                n_table_items.append(0)
        return n_table_items
    #"""

    # With Payload.
    def create_item(self, driver, folder_name, file_urls, document_names, payloads):
        if not os.path.exists(self.failed_downloads_path + folder_name):
            os.mkdir(self.failed_downloads_path + folder_name)

        item = DownloadFilesItem()
        item['file_urls'] = file_urls
        item['document_names'] = document_names
        item['payloads'] = payloads

        cookies = driver.get_cookies()
        print("cookies:", cookies) if PRINT else None
        item['session_cookies'] = cookies
        return item

    # Christchurch:
    def parse_data_item_Christchurch(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        try:
            if 'Disclaimer' in response.xpath('//*[@id="aspnetForm"]/div[3]/h2[1]/text()').get():
                print('Click: Accept.')
                driver.find_element(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_btnAccept"]').click()
        except TypeError:
            pass
        print(f"parse_data_item_Christchurch scraper name: {scraper_name}")

        folder_name = self.setup_storage_path(app_df)


    # DorsetCouncil:
    # https://planning.dorsetcouncil.gov.uk/plandisp.aspx?recno=393413
    # https://planning.dorsetcouncil.gov.uk/plandisp.aspx?recno=382798
    def parse_data_item_DorsetCouncil(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta["driver"]
        scraper_name = app_df.at['scraper_name']
        folder_name = self.setup_storage_path(app_df)
        max_file_name_len = self.max_folder_file_name_len - len(folder_name) - 5  # 5 chars for suffix/extension, such as .pdf
        try:
            disclaimer_index = 2 if scraper_name == 'DorsetCouncil' else 1
            if 'Disclaimer' in response.xpath(f'//*[@id="aspnetForm"]/div[3]/h2[{disclaimer_index}]/text()').get():
                print('Click: Accept.')
                driver.find_element(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_btnAccept"]').click()
        except TypeError:
            pass

        print(f'parse_data_item_DorsetCouncil, scraper name: {scraper_name}, max_file_name_len: {max_file_name_len}.')

        try:
            # --- --- --- Main Details --- --- ---
            tab_panel = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="planningdetails_wrapper"]')))
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
        detail_blocks = tab_panel.find_elements(By.XPATH, './div')

        items, item_values = [], []
        for detail_block in detail_blocks:
            items.extend(detail_block.find_elements(By.XPATH, './span'))
            item_values.extend(detail_block.find_elements(By.XPATH, './p'))
        n_items = len(items)
        print(f'\n1. Main Details Tab: {n_items} items from {len(detail_blocks)} blocks.')
        # test for further re-organization.
        #app_df = self.scrape_data_items(app_df, items[:-1], item_values[:-1])  # Exclude the last one: ' '
        app_df = scrape_data_items(app_df, items[:-1], item_values[:-1], self.details_dict, PRINT)  # Exclude the last one: ' '

        tabs = driver.find_elements(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_RadTabStrip1"]/div/ul/li/a')
        app_df = self.set_default_items(app_df)
        for tab_index, tab in enumerate(tabs[1:]):
            tab.click()
            tab_name = tab.text.strip()
            # print(f"tab {tab_index + 1}: {tab_name}")
            # --- --- --- Location (data + 'Ward Members' csv) --- --- ---
            if 'Location' in tab_name:
                location_blocks = driver.find_elements(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_pvLocation"]/div')
                items, item_values = [], []
                for location_block in location_blocks:
                    items.extend(location_block.find_elements(By.XPATH, './span'))
                    item_values.extend(location_block.find_elements(By.XPATH, './p'))
                n_items = len(items)
                print(f'\n{tab_index+2}. {tab_name} Tab: {n_items} items.')
                for item, value in zip(items, item_values):
                    item_name = item.text.strip()
                    data_name = self.details_dict[item_name]
                    item_value = value.text.strip()
                    try:
                        if data_name in ['Ward Members']:
                            if item_value == 'Find out who your ward councillor is':
                                print(f"    <{item_name}> scraped (ward members): None.") if PRINT else None
                            else:
                                ward_member_strings = item_value.split('\n')
                                print(f'    {ward_member_strings}')
                                ward_member_names = []
                                for member_index in range(len(ward_member_strings)):
                                    if len(ward_member_strings[member_index]) > 0:
                                        try:
                                            ward_member_name = ward_member_strings[member_index].split('Cllr')[1].strip()
                                        except IndexError:
                                            ward_member_name = ward_member_strings[member_index].split('Clrr')[1].strip()
                                        print(f'        Ward Member {member_index+1}: {ward_member_name}')
                                        ward_member_names.append(ward_member_name)
                                    else:
                                        break
                                pd.DataFrame({'Ward Members': ward_member_names}).to_csv(f"{self.data_storage_path}{folder_name}/ward_members.csv", index=False, quoting=1)
                                """
                                    ward_url = value.find_element(By.XPATH, './a').get_attribute('href')
                                    print(ward_url)
                                    assert ward_url == 'https://www.dorsetforyou.gov.uk/councillors' # DorsetCouncil
                                    assert ward_url == 'https://democracy.bcpcouncil.gov.uk/mgMemberIndex.aspx?bcr=1 # Christchurch
                                    # """
                                print(f'    <{item_name}> scraped (ward members): {ward_member_names}') if PRINT else None
                        else:
                            app_df.at[data_name] = item_value
                            print(f'    <{item_name}> scraped: {app_df.at[data_name]}') if PRINT else None
                    except KeyError:
                        app_df[data_name] = item_value
                        print(f'    <{item_name}> scraped (new): {app_df.at[data_name]}') if PRINT else None
            # --- --- --- View Documents (doc) --- --- ---  # Empty table with one row: There are currently no scanned documents for this application.
            elif 'Documents' in tab_name:
                def get_documents():
                    table_path = '//*[@id="ctl00_ContentPlaceHolder1_pvDocuments"]/table' if scraper_name == 'DorsetCouncil' else '//*[@id="ctl00_ContentPlaceHolder1_DocumentsGrid_ctl00"]'
                    document_table = driver.find_element(By.XPATH, table_path)
                    document_items = document_table.find_elements(By.XPATH, './tbody/tr')
                    if len(document_items[0].find_elements(By.XPATH, './td')) == 1:
                        print(f'\n{tab_index+2}. Document Tab: 0 items')
                        return 0, [], [], []
                    n_items = len(document_items)
                    print(f'\n{tab_index+2}. Document Tab: {n_items} items')

                    n_documents, file_urls, document_names, payloads = 0, [], [], []
                    for document_item in document_items:
                        n_documents += 1
                        file_url = app_df.at['url'] # +'?recno=30944'
                        file_urls.append(file_url)

                        #record_id = file_url.split('recno=')[-1] # 30944
                        print(f'    - - - Document {n_documents} - - - {file_url}')
                        VIEWSTATE = driver.find_element(By.XPATH, '//*[@id="__VIEWSTATE"]').get_attribute('value').strip()
                        #print('VIEWSTATE: ', VIEWSTATE)
                        EVENTTARGET = 'ctl00$ContentPlaceHolder1$DocumentsGrid'  # driver.find_element(By.XPATH, '//*[@id="__EVENTTARGET"]').get_attribute('value').strip()  # 'ctl00$ContentPlaceHolder1$DocumentsGrid'
                        #print('EVENTTARGET: ', EVENTTARGET)
                        EVENTARGUMENT = f'RowClicked:{n_documents-1}'  # driver.find_element(By.XPATH, '//*[@id="__EVENTARGUMENT"]').get_attribute('value')
                        #print('EVENTARGUMENT:', EVENTARGUMENT)
                        VIEWSTATEGENERATOR = driver.find_element(By.XPATH, '//*[@id="__VIEWSTATEGENERATOR"]').get_attribute('value').strip()  # 2DBBAB01
                        #print('VIEWSTATEGENERATOR: ', VIEWSTATEGENERATOR)
                        EVENTVALIDATION = driver.find_element(By.XPATH, '//*[@id="__EVENTVALIDATION"]').get_attribute('value').strip()
                        #print('EVENTVALIDATION: ', EVENTVALIDATION)
                        ctl00_ContentPlaceHolder1_RadTabStrip1_ClientState = driver.find_element(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_RadTabStrip1_ClientState"]').get_attribute('value').strip()
                        #print(ctl00_ContentPlaceHolder1_RadTabStrip1_ClientState)  # {&quot;selectedIndexes&quot;:[&quot;2&quot;],&quot;logEntries&quot;:[],&quot;scrollState&quot;:{}}
                        ctl00_ContentPlaceHolder1_RadMultiPage1_ClientState = driver.find_element(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_RadMultiPage1_ClientState"]').get_attribute('value').strip()
                        #print(ctl00_ContentPlaceHolder1_RadMultiPage1_ClientState)  # {&quot;selectedIndex&quot;:2,&quot;changeLog&quot;:[]}
                        payload = {
                            #'recno': record_id,
                            '__EVENTTARGET': EVENTTARGET,
                            '__EVENTARGUMENT': EVENTARGUMENT,
                            '__VIEWSTATE': VIEWSTATE,
                            '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
                            '__EVENTVALIDATION': EVENTVALIDATION,
                            'ctl00_ContentPlaceHolder1_RadTabStrip1_ClientState': ctl00_ContentPlaceHolder1_RadTabStrip1_ClientState,
                            'ctl00_ContentPlaceHolder1_DocumentsGrid_ClientState': '',
                            'ctl00_ContentPlaceHolder1_RadMultiPage1_ClientState': ctl00_ContentPlaceHolder1_RadMultiPage1_ClientState,
                            'ctl00$ContentPlaceHolder1$tbName': '',
                            'ctl00$ContentPlaceHolder1$tbEmailAddress': '',
                        }
                        payloads.append(payload)

                        document_name_string = document_item.find_element(By.XPATH, './td[2]/a').text.strip()
                        document_name_strings = document_name_string.split('.')
                        print(f'    {document_name_strings}') if PRINT else None

                        # No item_extension, get it from pipeline, response.headers
                        document_name_strings = document_name_strings[0].split('-')
                        document_date = document_name_strings[0].strip()
                        document_description = document_name_strings[-1].strip()
                        document_name = f'date={document_date}&desc={document_description}&uid={n_documents}'
                        len_limitation = len(document_name) - max_file_name_len
                        print(f'    Doc {n_documents} len_limitation: {len_limitation}') if len_limitation > -5 else None
                        if len_limitation > 0:
                            document_description = document_description[:-len_limitation]
                            document_name = f'date={document_date}&desc={document_description}&uid={n_documents}'
                        print(f'    Document {n_documents}: {document_name}') if PRINT else None

                        document_name = replace_invalid_characters(document_name)
                        # print('new: ', document_name) if PRINT else None
                        document_names.append(f"{self.data_upload_path}{folder_name}/{document_name}")
                    app_df.at['other_fields.n_documents'] = n_documents
                    #print(f'Total documents: {n_documents}') if PRINT else None
                    return n_documents, file_urls, document_names, payloads
                n_documents, file_urls, document_names, payloads = get_documents()
                if n_documents > 0:
                    item = self.create_item(driver, folder_name, file_urls, document_names, payloads)
                    yield item
            # --- --- --- Consultees (multi-table csv) --- --- --- # Similar to Atrium, but the first table locates at a sub-layer.
            elif 'Consultees' in tab_name:
                def parse_consultees():
                    # check tables: neighbour list, consultee list, public notices ...
                    table_list = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_pvConsultees"]')))
                    table_names = table_list.find_element(By.XPATH, './div[1]').find_elements(By.XPATH, './span')
                    table_names.extend(table_list.find_elements(By.XPATH, './span'))

                    table_path = './div/table'
                    tables = table_list.find_element(By.XPATH, './div[1]').find_elements(By.XPATH, table_path)
                    tables.extend(table_list.find_elements(By.XPATH, table_path))

                    n_tables = len(tables)
                    print(f'\n{tab_index+2}. {tab_name} Tab: {n_tables} tables.') if PRINT else None
                    table_name_dict = {'Neighbour List': 'neighbour comments',
                                       'Consultee List': 'consultee comments',
                                       'Public Notices': 'public notices'}
                    csv_names = [table_name_dict[table_name.text.strip()] for table_name in table_names]
                    # test for further re-organization.
                    #n_table_items = self.scrape_multi_tables_for_csv(csv_names, tables, folder_name, table_path='tbody/tr', column_path='th', item_path='td')
                    n_table_items = scrape_multi_tables_for_csv(csv_names, tables, self.data_storage_path, folder_name, table_path='tbody/tr', column_path='th', item_path='td', PRINT=PRINT)

                    for csv_name, n_items in zip(csv_names, n_table_items):
                        if csv_name == 'neighbour comments':
                            app_df.at['other_fields.n_comments_public_received'] = n_items
                        elif csv_name == 'consultee comments':
                            app_df.at['other_fields.n_comments_consultee_responded'] = n_items
                        elif csv_name == 'public notices':
                            app_df.at['other_fields.n_public_notices'] = n_items
                    app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received'] + app_df.at['other_fields.n_comments_consultee_responded']
                    print(f"number of comments: {app_df.at['other_fields.n_comments']}")
                parse_consultees()
            # --- --- --- Appeals (data + csv) --- --- ---
            # Validated on both: data items, empty table.
            # Not validated: Non-empty table.
            elif 'Appeals' in tab_name:
                def parse_appeals():
                    appeal_panel = driver.find_element(By.XPATH, '//*[@id="ctl00_ContentPlaceHolder1_pvAppeals"]')
                    appeal_blocks = appeal_panel.find_elements(By.XPATH, './span/span/div')

                    items, item_values = [], []
                    for appeal_block in appeal_blocks:
                        items.extend(appeal_block.find_elements(By.XPATH, './span'))
                        item_values.extend(appeal_block.find_elements(By.XPATH, './p'))
                    n_items = len(items)
                    print(f'\n{tab_index+2}. {tab_name} Tab: {n_items} data items.')
                    for item, value in zip(items, item_values):
                        item_name = f'Appeal_{item.text.strip()}'
                        data_name = self.details_dict[item_name]
                        item_value = value.text.strip()
                        try:
                            app_df.at[data_name] = item_value
                            print(f"    <{item_name}> scraped: {app_df.at[data_name]}") if PRINT else None
                        # New
                        except KeyError:
                            app_df[data_name] = item_value
                            print(f"    <{item_name}> scraped (new): {app_df.at[data_name]}") if PRINT else None
                    try:
                        if n_items > 0:
                            appeal_path = './div/table' if scraper_name == 'DorsetCouncil' else './div/div/table'
                        else: # No appeals.
                            appeal_path = './div/div/table' if scraper_name == 'DorsetCouncil' else './div/div/div/table'
                        appeal_table = appeal_panel.find_element(By.XPATH, appeal_path)
                        table_items = appeal_table.find_elements(By.XPATH, './tbody/tr')
                        table_columns = table_items[0].find_elements(By.XPATH, './th')
                        if len(table_columns) > 0:
                            n_items = len(table_items) - 1  # exclude the column row.
                            print(f'{tab_index+2}. {tab_name} Tab: {n_items} table items.')  # if PRINT else None
                            # test for further re-organization.
                            # self.scrape_for_csv(csv_name='appeals', table_columns=table_columns, table_items=table_items[1:], folder_name=folder_name, path='td')
                            scrape_for_csv(csv_name='appeals', table_columns=table_columns, table_items=table_items[1:], data_storage_path=self.data_storage_path, folder_name=folder_name, path='td')
                        else:
                            print(f"{tab_index+2}. " + table_items[0].find_element(By.XPATH, f'./td').text.strip())
                    except NoSuchElementException:
                        appeal_table = appeal_panel.find_element(By.XPATH, './div/table')
                        table_items = appeal_table.find_elements(By.XPATH, './tbody/tr')
                        assert len(table_items) == 1 and table_items[0].text.strip() == 'There are currently no scanned documents for this appeal.'
                        print(f'{tab_index+2}. {tab_name} Tab: NULL table items.')
                parse_appeals()
            # --- --- --- History (csv) --- --- ---
            elif 'History' in tab_name:
                def parse_history():
                    try:
                        table = driver.find_element(By.XPATH, f'//*[@id="ctl00_ContentPlaceHolder1_gridLinks"]')
                        table_items = table.find_elements(By.XPATH, './tbody/tr')
                        table_columns = table_items[0].find_elements(By.XPATH, './th')
                        if len(table_columns) > 0:
                            n_items = len(table_items) - 1  # exclude the column row.
                            print(f'\n{tab_index+2}. {tab_name} Tab: {n_items} items.')
                            # test for further re-organization.
                            # self.scrape_for_csv(csv_name='history', table_columns=table_columns, table_items=table_items[1:], folder_name=folder_name, path='td')
                            scrape_for_csv(csv_name='history', table_columns=table_columns, table_items=table_items[1:], data_storage_path=self.data_storage_path, folder_name=folder_name, path='td')
                        else:
                            print(f"\n{tab_index+2}. " + table_items[0].find_element(By.XPATH, f'./td').text.strip())
                    except NoSuchElementException:
                        print(f'\n{tab_index+2}. {tab_name} Tab: NULL.')
                parse_history()
            # --- --- --- Unknown Tabs --- --- ---
            else:
                print(f"{tab_index+2}. Unknown Tab: {tab_name}.")
                assert 1 == 0
        self.ending(app_df)
