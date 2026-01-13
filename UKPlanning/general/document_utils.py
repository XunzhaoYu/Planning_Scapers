
import os, re
import numpy as np
from general.items import DownloadFilesItem
from selenium.webdriver.common.by import By
from configs.settings import PRINT
from general.utils import get_data_storage_path, replace_invalid_characters


def get_document_info_columns(response):
    columns = response.xpath('//*[@id="Documents"]/tbody/tr[1]/th')
    n_columns = len(columns)
    date_column = n_columns
    type_column = n_columns
    description_column = n_columns
    for i, column in enumerate(columns):
        try:
            if 'date' in str.lower(column.xpath('./a/text()').get()):
                date_column = i + 1
                continue
            if 'type' in str.lower(column.xpath('./a/text()').get()):
                type_column = i + 1
                continue
            if 'description' in str.lower(column.xpath('./a/text()').get()):
                description_column = i + 1
                continue
        except TypeError:
            continue
    print(f"date column {date_column}, type column {type_column}, description column {description_column}, n_columns {n_columns}") if PRINT else None
    return date_column, type_column, description_column

# similar to the rename_documents() in scrape_documents_by_checkbox(), but without: 1>. clicking sort button 2>. pair un-matched documents.
def get_documents(response, folder_path, folder_name):  # All docs in one page.
    date_column, type_column, description_column = get_document_info_columns(response)
    document_items = response.xpath('//*[@id="Documents"]/tbody/tr')[1:]
    document_names = []
    file_urls = []
    for i, document_item in enumerate(document_items):
        try:
            file_url = document_item.xpath('./td/a')[-1].xpath('./@href').get()
            if file_url is None:
                continue
        except IndexError:
            continue
        # file_url = document_item.css('a::attr(href)').get()

        document_date = document_item.xpath(f'./td[{date_column}]/text()').get().strip()
        document_type = document_item.xpath(f'./td[{type_column}]/text()').get().strip()
        try:
            document_description = document_item.xpath(f'./td[{description_column}]/text()').get().strip()
        except AttributeError:
            document_description = ''
        """ # the docs downloaded by file links are different from the docs downloaded from download button (.zip). Set extensions could results in crashed docs.
        try:
            item_identity = document_item.xpath('./td/input')[0].xpath('./@value').get().strip().split('-')[-1]
            print(i, item_identity)
        except TypeError:
            item_identity = file_url.split('-')[-1]
        """
        item_identity = file_url.split('-')[-1]
        document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
        print(document_name) if PRINT else None
        document_name = replace_invalid_characters(document_name)
        document_names.append(f"{folder_path}{folder_name}/{document_name}")
        file_urls.append(response.urljoin(file_url))
    return file_urls, document_names

# Updated on 04/06/2024
def get_Civica_documents(response, document_items, n_documents, folder_path, folder_name, Civica_version=2024):
    file_urls = []
    document_names = []
    if Civica_version == 2024:  # Scrape using selenium driver.
        for i, document_item in enumerate(document_items):
            # print(document_item.text)
            print(f'--- --- document {i+1} --- ---') if PRINT else None
            file_url = document_item.find_element(By.XPATH, './a').get_attribute('href')
            file_urls.append(response.urljoin(file_url))
            #print(file_url)

            item_identity = file_url.split('=')[-1]
            document_date = document_item.find_element(By.CLASS_NAME, 'civica-doclistdetailtext').text
            print('date: ', document_date) if PRINT else None
            document_description = document_item.find_element(By.CLASS_NAME, 'civica-doclisttitletext').text
            print('description: ', document_description) if PRINT else None
            document_name = f"date={document_date}&desc={document_description}&uid={item_identity}"

            # Check the format of document names.
            print(document_name) if PRINT else None
            document_name = replace_invalid_characters(document_name)
            document_names.append(f"{folder_path}{folder_name}/{document_name}.pdf")
    elif Civica_version == 2006:  # to be completed
        for i, document_item in enumerate(document_items):
            # print(document_item.text)
            print(f'--- --- document {i+1} --- ---') if PRINT else None

            file_url = document_item.find_element(By.XPATH, './a').get_attribute('href')
            file_urls.append(response.urljoin(file_url))
            # print(file_url)

            ###item_identity = file_url.split('=')[-1]
            document_date = document_item.find_element(By.XPATH, './td[2]').text
            print('date: ', document_date) if PRINT else None
            document_description = document_item.find_element(By.XPATH, './td[1]').text
            print('description: ', document_description) if PRINT else None
            document_name = f"date={document_date}&desc={document_description}&uid={item_identity}"

            # Check the format of document names.
            print(document_name) if PRINT else None
            document_name = replace_invalid_characters(document_name)
            document_names.append(f"{folder_path}{folder_name}/{document_name}.pdf")
    else: # Unknown Civica version
        pass
    return file_urls, document_names

def generate_unique_document_name(existing_names, document_name):
    # Note that the document_name should not contain any extension such as '.pdf'
    if document_name in existing_names:
        base = document_name
        rename_index = 2
        while True:
            document_name = base + str(rename_index)
            if document_name not in existing_names:
                return document_name
            else:
                rename_index += 1
    else:
        return document_name

# Updated on 03/06/2024. for Idox scrapers
def get_NEC_or_Northgate_documents_Idox(response, n_documents, folder_path, folder_name, version=2024):
    file_urls = []
    document_names = []
    if version == 2024:  # Scrape from Javascript code. All docs in one page.
        try:
            javascript = response.xpath('//*[@id="searchResult"]/script[4]/text()').get().strip()
        except AttributeError:
            try:
                javascript = response.xpath('//*[@id="searchResult"]/script[6]/text()').get().strip()
            except AttributeError:
                javascript = response.xpath('///*[@id="layoutMain"]/div/div/script[4]/text()').get().strip()
        #print(javascript)

        ### Extract document uid. ###
        id_pattern = r'"Guid":"[0-9A-F]+",'  # hexadecimal: 0-9, A-F.
        doc_ids = re.findall(id_pattern, javascript)
        doc_ids = [doc_id[8:-2] for doc_id in doc_ids]
        if len(doc_ids) != n_documents:
            print(len(doc_ids))
        assert(len(doc_ids) == n_documents)
        print(doc_ids) if PRINT else None

        ### Extract document details: [compulsory]: date, type, [optional]: description, file type, etc. ###
        date_pattern = '"Date_Received":"\d{2}/\d{2}/\d{4}'  # MM-DD-YYYY
        document_dates = re.findall(date_pattern, javascript)
        document_dates = [f"{document_date[20:22]} {document_date[17:19]} {document_date[23:27]}" for document_date in document_dates]

        # Type
        type_pattern = '"Doc_Type":"[^"]*"'
        document_types = re.findall(type_pattern, javascript)
        document_types = [document_type[12:-1].strip() for document_type in document_types]

        DESC, DESC2, FILETYPE = False, False, False
        # Optional: Description
        description_pattern = '"Doc_Ref2":"[^"]*"'
        document_descriptions = re.findall(description_pattern, javascript)
        if len(document_descriptions) > 0:
            DESC = True
            document_descriptions = [document_description[12:-1].strip() for document_description in document_descriptions]

        # Optional: Description2
        description2_pattern = '"Doc_Ref":"[^"]*"'
        document_descriptions2 = re.findall(description2_pattern, javascript)
        if len(document_descriptions2) > 0:
            DESC2 = True
            document_descriptions2 = [document_description2[11:-1].strip() for document_description2 in document_descriptions2]

        # Optional: File Type
        filetype_pattern = '"FileType":".\w*"}'
        document_filetypes = re.findall(filetype_pattern, javascript)
        if len(document_filetypes) > 0:
            FILETYPE = True
            document_filetypes = [document_filetype[12:-2].lower() for document_filetype in document_filetypes]
        print(f"DESC: {DESC}, DESC2: {DESC2}, FILETYPE: {FILETYPE}") if PRINT else None

        ### Get url domain ###
        url_domain = response.url.split('/')[2]
        print("url domain: ", url_domain) if PRINT else None

        ### Generate file urls: url domain + document uids; Generate document names ###
        view_document_url_pattern = 'var viewDocumentUrl = [^;]+;'
        view_document_url = re.findall(view_document_url_pattern, javascript)[0].split('=')[1]
        view_document_url = view_document_url[2:-2]
        print(view_document_url) if PRINT else None
        #view_document_url = '/AniteIM.WebSearch/Document/ViewDocument'  # NEC
        #view_document_url = '/PublicAccess_LIVE/Document'  # Northgate

        for doc_index in range(n_documents):
            file_url = f"https://{url_domain}{view_document_url}?id={doc_ids[doc_index]}"
            file_urls.append(file_url)
            print(file_url) if PRINT else None

        for doc_index in range(n_documents):
            # Load compulsory details: date and doc type.
            document_name = f"date={document_dates[doc_index]}&type={document_types[doc_index]}"
            # Load optional details:
            if DESC and not DESC2:
                if len(document_descriptions[doc_index]) > 0:
                    document_name = f"{document_name}&desc={document_descriptions[doc_index]}"
            elif DESC2 and not DESC:
                if len(document_descriptions2[doc_index]) > 0:
                    document_name = f"{document_name}&desc={document_descriptions2[doc_index]}"
            elif DESC and DESC2:
                if len(document_descriptions[doc_index]) > 0 and len(document_descriptions2[doc_index]) > 0:
                    if document_descriptions[doc_index] == document_descriptions2[doc_index]:
                        document_name = f"{document_name}&desc={document_descriptions[doc_index]}"
                    else:
                        document_name = f"{document_name}&desc={document_descriptions[doc_index]}&desc2={document_descriptions2[doc_index]}"
                elif len(document_descriptions[doc_index]) > 0:
                    document_name = f"{document_name}&desc={document_descriptions[doc_index]}"
                elif len(document_descriptions2[doc_index]) > 0:
                    document_name = f"{document_name}&desc={document_descriptions2[doc_index]}"
                else:
                    pass  # Document descriptions are empty.
            else:
                pass  # No document description.
            if FILETYPE:
                document_name = f"{document_name}&filetype={document_filetypes[doc_index]}"
            document_name = f"{document_name}&uid={doc_ids[doc_index]}"
            #document_name = f"date={document_dates[doc_index]}&type={document_types[doc_index]}({document_filetypes[doc_index].lower()})&desc={document_descriptions[doc_index]}"

            # Check the format of document names.
            document_name = document_name.encode('utf-8').decode('unicode_escape')  # some document names may contain characters presented in other unicode, e.g. \u0026
            document_name = document_name.replace('\r', ' ').replace('\n', ' ').strip()
            print(document_name) if PRINT else None
            document_name = replace_invalid_characters(document_name)
            #document_name = generate_unique_document_name(existing_names, document_name)
            #existing_names.append(document_name)
            document_names.append(f"{folder_path}{folder_name}/{document_name}.pdf")
    elif version == 2009:  # Default 25 docs per page, max 50 docs per page. Need to change pages if there are many docs.
        ### Get url domain ###
        url_domain = response.url.split('/')[2]
        print("url domain: ", url_domain) if PRINT else None
        n_pages = np.ceil(n_documents/25)
        if n_pages > 1:
            driver = response.request.meta["driver"]
        for page in range(n_pages):
            document_items = response.xpath('//*[@id="grdResults_tblData"]/tbody/tr')[1:]
            for i, document_item in enumerate(document_items):
                document_date = document_item.xpath(f'./td[1]/text()').get().strip()
                document_type = document_item.xpath(f'./td[2]/text()').get().strip()
                try:
                    document_description = document_item.xpath(f'./td[4]/text()').get().strip()
                except AttributeError:
                    document_description = ''
                item_identity = document_item.xpath(f'./td[5]/text()').get().strip()
                document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
                print(document_name) if PRINT else None
                document_name = replace_invalid_characters(document_name)
                document_names.append(f"{folder_path}{folder_name}/{document_name}")

                file_url = f"https://{url_domain}/AnitePublicDocs/00{item_identity}.pdf"
                file_urls.append(file_url)
                print(file_url) if PRINT else None
            if page > 0:
                driver.find_element(By.XPATH, '//*[@id="grdResults__ctl0_cmdNext"]').click()
    else: # Unknown NEC or Northgate version
        pass

    return file_urls, document_names


# Updated on 12/01/2026. for Agile scrapers
def get_NEC_or_Northgate_documents(driver, n_documents, folder_path, folder_name, version=2024):
    file_urls = []
    document_names = []
    if version == 2024:  # Scrape from Javascript code. All docs in one page.
        #javascript = driver.find_element(By.XPATH, '//*[@id="searchResult"]/script[4] | //*[@id="searchResult"]/script[6] | //*[@id="layoutMain"]/div/div/script[4]').text.strip()
        javascript = driver.find_element(By.XPATH, '//*[@id="searchResult"]/script[6]').get_attribute('innerHTML')
        #driver.execute_script("return model;")
        # //*[@id="searchResult"]/script[6]/text()
        print(javascript)

        ### Extract document uid. ###
        id_pattern = r'"Guid":"[0-9A-F]+",'  # hexadecimal: 0-9, A-F.
        doc_ids = re.findall(id_pattern, javascript)
        doc_ids = [doc_id[8:-2] for doc_id in doc_ids]
        if len(doc_ids) != n_documents:
            print(len(doc_ids))
        assert(len(doc_ids) == n_documents)
        print(doc_ids) if PRINT else None

        ### Extract document details: [compulsory]: date, type, [optional]: description, file type, etc. ###
        date_pattern = '"Date_Received":"\d{2}/\d{2}/\d{4}'  # MM-DD-YYYY
        document_dates = re.findall(date_pattern, javascript)
        document_dates = [f"{document_date[20:22]} {document_date[17:19]} {document_date[23:27]}" for document_date in document_dates]

        # Type
        type_pattern = '"Doc_Type":"[^"]*"'
        document_types = re.findall(type_pattern, javascript)
        document_types = [document_type[12:-1].strip() for document_type in document_types]

        DESC, DESC2, FILETYPE = False, False, False
        # Optional: Description
        description_pattern = '"Doc_Ref2":"[^"]*"'
        document_descriptions = re.findall(description_pattern, javascript)
        if len(document_descriptions) > 0:
            DESC = True
            document_descriptions = [document_description[12:-1].strip() for document_description in document_descriptions]

        # Optional: Description2
        description2_pattern = '"Doc_Ref":"[^"]*"'
        document_descriptions2 = re.findall(description2_pattern, javascript)
        if len(document_descriptions2) > 0:
            DESC2 = True
            document_descriptions2 = [document_description2[11:-1].strip() for document_description2 in document_descriptions2]

        # Optional: File Type
        filetype_pattern = '"FileType":".\w*"}'
        document_filetypes = re.findall(filetype_pattern, javascript)
        if len(document_filetypes) > 0:
            FILETYPE = True
            document_filetypes = [document_filetype[12:-2].lower() for document_filetype in document_filetypes]
        print(f"DESC: {DESC}, DESC2: {DESC2}, FILETYPE: {FILETYPE}") if PRINT else None

        ### Get url domain ###
        url_domain = response.url.split('/')[2]
        print("url domain: ", url_domain) if PRINT else None

        ### Generate file urls: url domain + document uids; Generate document names ###
        view_document_url_pattern = 'var viewDocumentUrl = [^;]+;'
        view_document_url = re.findall(view_document_url_pattern, javascript)[0].split('=')[1]
        view_document_url = view_document_url[2:-2]
        print(view_document_url) if PRINT else None
        #view_document_url = '/AniteIM.WebSearch/Document/ViewDocument'  # NEC
        #view_document_url = '/PublicAccess_LIVE/Document'  # Northgate

        for doc_index in range(n_documents):
            file_url = f"https://{url_domain}{view_document_url}?id={doc_ids[doc_index]}"
            file_urls.append(file_url)
            print(file_url) if PRINT else None

        for doc_index in range(n_documents):
            # Load compulsory details: date and doc type.
            document_name = f"date={document_dates[doc_index]}&type={document_types[doc_index]}"
            # Load optional details:
            if DESC and not DESC2:
                if len(document_descriptions[doc_index]) > 0:
                    document_name = f"{document_name}&desc={document_descriptions[doc_index]}"
            elif DESC2 and not DESC:
                if len(document_descriptions2[doc_index]) > 0:
                    document_name = f"{document_name}&desc={document_descriptions2[doc_index]}"
            elif DESC and DESC2:
                if len(document_descriptions[doc_index]) > 0 and len(document_descriptions2[doc_index]) > 0:
                    if document_descriptions[doc_index] == document_descriptions2[doc_index]:
                        document_name = f"{document_name}&desc={document_descriptions[doc_index]}"
                    else:
                        document_name = f"{document_name}&desc={document_descriptions[doc_index]}&desc2={document_descriptions2[doc_index]}"
                elif len(document_descriptions[doc_index]) > 0:
                    document_name = f"{document_name}&desc={document_descriptions[doc_index]}"
                elif len(document_descriptions2[doc_index]) > 0:
                    document_name = f"{document_name}&desc={document_descriptions2[doc_index]}"
                else:
                    pass  # Document descriptions are empty.
            else:
                pass  # No document description.
            if FILETYPE:
                document_name = f"{document_name}&filetype={document_filetypes[doc_index]}"
            document_name = f"{document_name}&uid={doc_ids[doc_index]}"
            #document_name = f"date={document_dates[doc_index]}&type={document_types[doc_index]}({document_filetypes[doc_index].lower()})&desc={document_descriptions[doc_index]}"

            # Check the format of document names.
            document_name = document_name.encode('utf-8').decode('unicode_escape')  # some document names may contain characters presented in other unicode, e.g. \u0026
            document_name = document_name.replace('\r', ' ').replace('\n', ' ').strip()
            print(document_name) if PRINT else None
            document_name = replace_invalid_characters(document_name)
            #document_name = generate_unique_document_name(existing_names, document_name)
            #existing_names.append(document_name)
            document_names.append(f"{folder_path}{folder_name}/{document_name}.pdf")
    elif version == 2009:  # Default 25 docs per page, max 50 docs per page. Need to change pages if there are many docs.
        ### Get url domain ###
        url_domain = response.url.split('/')[2]
        print("url domain: ", url_domain) if PRINT else None
        n_pages = np.ceil(n_documents/25)
        if n_pages > 1:
            driver = response.request.meta["driver"]
        for page in range(n_pages):
            document_items = response.xpath('//*[@id="grdResults_tblData"]/tbody/tr')[1:]
            for i, document_item in enumerate(document_items):
                document_date = document_item.xpath(f'./td[1]/text()').get().strip()
                document_type = document_item.xpath(f'./td[2]/text()').get().strip()
                try:
                    document_description = document_item.xpath(f'./td[4]/text()').get().strip()
                except AttributeError:
                    document_description = ''
                item_identity = document_item.xpath(f'./td[5]/text()').get().strip()
                document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
                print(document_name) if PRINT else None
                document_name = replace_invalid_characters(document_name)
                document_names.append(f"{folder_path}{folder_name}/{document_name}")

                file_url = f"https://{url_domain}/AnitePublicDocs/00{item_identity}.pdf"
                file_urls.append(file_url)
                print(file_url) if PRINT else None
            if page > 0:
                driver.find_element(By.XPATH, '//*[@id="grdResults__ctl0_cmdNext"]').click()
    else: # Unknown NEC or Northgate version
        pass

    return file_urls, document_names





# doc href is #.
def get_Exeter_documents(response, document_tree, n_documents, folder_path, folder_name):
    file_urls = []
    document_names = []
    document_items = document_tree.find_elements(By.XPATH, '//*[@id="1"]')
    pre_file_name_len = len(f"{get_data_storage_path()}{folder_path}{folder_name}/")
    MAX_FILE_NAME_LEN = 210
    allowed_file_name_len = MAX_FILE_NAME_LEN - pre_file_name_len
    #print(f"{get_data_storage_path()}{folder_path}{folder_name}/", pre_file_name_len, allowed_file_name_len)
    url_pattern = r"window\.open\('([^']+)'"
    for i, document_item in enumerate(document_items):
        # print(document_item.text)
        print(f'--- --- document {i+1} --- ---') if PRINT else None
        file_url = document_item.find_element(By.XPATH, './a').get_attribute('onclick')
        #file_url2 = re.search(r"window\.open\('([^']+)'", file_url).group(1)
        #print(file_url2)
        file_url = re.findall(url_pattern, file_url)[0]
        file_urls.append(response.urljoin(file_url))
        #print(file_url)

        document_string = document_item.find_element(By.XPATH, './a').get_attribute('title')
        print(document_string)
        document_date, document_description = document_string.split("_", 1)
        print('date: ', document_date) if PRINT else None
        print('description: ', document_description) if PRINT else None
        #print(len(document_description))
        if len(document_description) > allowed_file_name_len:
            document_description = document_description[:allowed_file_name_len]
        suffix = file_url.split('/')[-1].split('_')[1].lower()
        document_name = f"date={document_date}&desc={document_description}&uid={i+1}.{suffix}"  # 7, 18
        #print(len(document_description), pre_file_name_len + len(document_description))

        # Check the format of document names.
        print(document_name) if PRINT else None
        document_name = replace_invalid_characters(document_name)
        #print('new: ', document_name) if PRINT else None
        document_names.append(f"{folder_path}{folder_name}/{document_name}")
    return file_urls, document_names









# Updated on 01/06/2024, has been merged to get_NEC_or_Northgate_documents().
#
# Similar to NEC, but it has no attributes such as file_type in document names.
# Different points (compared with NEC):
# 1. viewDocumentUrl
# 2. document features: Do not have features such as file types.
def get_Northgate_documents(response, n_documents, folder_path, folder_name):
    javascript = response.xpath('//*[@id="searchResult"]/script[4]/text()').get().strip()
    # print(javascript)

    ### Extract document uid. ###
    id_pattern = r'"Guid":"[0-9A-F]+",'  # hexadecimal: 0-9, A-F.
    doc_ids = re.findall(id_pattern, javascript)
    doc_ids = [doc_id[8:-2] for doc_id in doc_ids]
    print(len(doc_ids))
    assert (len(doc_ids) == n_documents)
    print(doc_ids)

    ### Extract document details: date, type, description, file type. ###
    date_pattern = '"Date_Received":"\d{2}/\d{2}/\d{4}'
    document_dates = re.findall(date_pattern, javascript)
    document_dates = [document_date[17:] for document_date in document_dates]

    type_pattern = '"Doc_Type":"[^"]*"'
    document_types = re.findall(type_pattern, javascript)
    document_types = [document_type[12:-1] for document_type in document_types]

    description_pattern = '"Doc_Ref2":"[^"]*"'
    document_descriptions = re.findall(description_pattern, javascript)
    document_descriptions = [document_description[12:-1] for document_description in document_descriptions]

    ### Get url domain ###
    url_domain = response.url.split('/')[2]
    print("url domain: ", url_domain) if PRINT else None

    ### Generate file urls: url domain + document uids; Generate document names ###
    view_document_url_pattern = 'var viewDocumentUrl = [^;]+;'
    view_document_url = re.findall(view_document_url_pattern, javascript)[0].split('=')[1]
    view_document_url = view_document_url[2:-2]
    print(view_document_url) if PRINT else None
    # view_document_url = '/PublicAccess_LIVE/Document'  # Northgate

    file_urls = []
    for doc_index in range(n_documents):
        file_url = f"https://{url_domain}{view_document_url}?id={doc_ids[doc_index]}"
        file_urls.append(file_url)
        print(file_url) if PRINT else None

    existing_names = []
    document_names = []
    for doc_index in range(n_documents):
        document_name = f"date={document_dates[doc_index]}&type={document_types[doc_index]}&desc={document_descriptions[doc_index]}.pdf"
        document_name = document_name.encode('utf-8').decode('unicode_escape')  # some document names may contain characters presented in other unicode, e.g. \u0026
        print(document_name) if PRINT else None
        document_name = replace_invalid_characters(document_name)
        document_name = generate_unique_document_name(existing_names, document_name)
        existing_names.append(document_name)
        document_names.append(f"{folder_path}{folder_name}/{document_name}")
    return file_urls, document_names


"""
The following are obsolete code and thus useless.
"""

def unzip_documents(self, storage_path, wait_unit=1.0, wait_total=100):
    zipname = ''
    n_wait = 0

    while zipname == '' and n_wait < wait_total:
        time.sleep(wait_unit)
        n_wait += wait_unit
        rootfiles = os.listdir(get_project_root())
        for filename in rootfiles:
            # print(f"{n_wait} checking: {filename}")
            if filename.endswith('.zip'):
                zipname = filename
                break
    print("{:.1f} secs, zipname: {:s}".format(n_wait, zipname))

    unzip_dir = f"{storage_path}documents/"
    with zipfile.ZipFile(zipname, 'r') as zip_ref:
        zip_ref.extractall(unzip_dir)
    os.remove(zipname)
    return unzip_dir

#driver = response.request.meta["driver"]
#checkboxs = driver.find_elements(By.NAME, 'file')
#self.scrape_documents_by_checkbox(response, driver, checkboxs, n_documents, storage_path)
def scrape_documents_by_checkbox(self, response, driver, checkboxs, n_documents, storage_path):
    n_checkboxs = len(checkboxs)

    def rename_documents():
        docfiles = os.listdir(unzip_dir)
        docfiles.sort(key=str.lower)
        date_column, type_column, description_column = self.get_document_info_columns(response)

        # Click 'Description' button to sort documents. 点击网页上description的按钮进行排序
        description_button = None
        Descending = False
        try:
            sorting_buttons = driver.find_elements(By.CLASS_NAME, 'ascending')
            # sorting_buttons = driver.find_elements(By.XPATH, '//*[@id="Documents"]/tbody/tr')[0]
            for sorting_button in sorting_buttons:
                if 'description' in str.lower(sorting_button.text):
                    description_button = sorting_button
                    break
            description_button.click()
        except AttributeError:
            try:
                sorting_buttons = driver.find_elements(By.CLASS_NAME, 'descending')
                for sorting_button in sorting_buttons:
                    if 'description' in str.lower(sorting_button.text):
                        description_button = sorting_button
                        break
                description_button.click()
                Descending = True
            except AttributeError:
                print(f"failed to sort documents items.")

        # 通过driver获取排序后的文档信息
        unpaired_bases = []
        unpaired_extensions = []
        unpaired_names = []
        unpaired_identities = []
        document_items = driver.find_elements(By.XPATH, '//*[@id="Documents"]/tbody/tr')[1:]
        if Descending:
            document_items = document_items[::-1]
        print("length comparison:", len(docfiles), len(document_items)) if PRINT else None
        for i, document_item in enumerate(document_items):
            # item_info = document_item.text
            # print(item_info)
            document_date = document_item.find_element(By.XPATH, f'./td[{date_column}]').text
            document_type = document_item.find_element(By.XPATH, f'./td[{type_column}]').text
            document_description = document_item.find_element(By.XPATH, f'./td[{description_column}]').text
            item_identity = document_item.find_elements(By.XPATH, './td/a')[-1].get_attribute('href').split('-')[-1]
            item_identity = item_identity.split('.')[0]  # remove the suffix. Some doc names end with .tif but their link names end with .pdf.
            document_name = f"date={document_date}&type={document_type}&desc={document_description}&{item_identity}"
            print(document_name) if PRINT else None
            document_name = replace_invalid_characters(document_name)

            docfile_base, docfile_extension = os.path.splitext(docfiles[i])
            if docfile_base.endswith(item_identity):
                os.rename(unzip_dir + docfiles[i], f"{storage_path}{document_name}{docfile_extension}")
            else:
                print(i + 1, "- - - ", docfile_base, docfile_extension) if PRINT else None
                unpaired_bases.append(docfile_base)
                unpaired_extensions.append(docfile_extension)
                unpaired_names.append(document_name)
                unpaired_identities.append(item_identity)

        # pair item_identity with the name of downloaded documents
        for docfile_base, docfile_extension in zip(unpaired_bases, unpaired_extensions):
            print(unpaired_names) if PRINT else None
            for i, identity in enumerate(unpaired_identities):
                if docfile_base.endswith(identity):
                    paired_name = unpaired_names[i]
                    os.rename(unzip_dir + docfile_base + docfile_extension, f"{storage_path}{paired_name}{docfile_extension}")
                    unpaired_names.remove(paired_name)
                    unpaired_identities.remove(identity)
                    continue
        os.rmdir(unzip_dir)

    max_checkboxs = 24
    n_downloads = int(np.ceil(n_checkboxs / max_checkboxs))
    n_full_downloads = n_downloads - 1
    print(f"Downloading {n_checkboxs} documents by {n_downloads} downloads ...")
    download_failure = False
    try:
        for i in range(n_full_downloads):
            start_index = i * max_checkboxs
            end_index = (i + 1) * max_checkboxs
            for checkbox in checkboxs[start_index: end_index]:
                checkbox.click()
            time.sleep(0.1)
            driver.find_element(By.ID, 'downloadFiles').click()
            for checkbox in checkboxs[start_index: end_index]:
                checkbox.click()
            # Unzip downloaded documents.
            unzip_dir = self.unzip_documents(storage_path)
    except FileNotFoundError as error:
        print("Downloading Failed:", error)
        download_failure = True

    try:
        start_index = n_full_downloads * max_checkboxs
        end_index = n_documents
        for checkbox in checkboxs[start_index: end_index]:
            checkbox.click()
        time.sleep(0.1)
        download_time = time.time()
        driver.find_element(By.ID, 'downloadFiles').click()
        print("Download button time cost {:.4f} secs.".format(time.time() - download_time)) if PRINT else None
        # Unzip downloaded documents.
        unzip_dir = self.unzip_documents(storage_path)
    except FileNotFoundError as error:
        print("Downloading Failed:", error)
        download_failure = True

    if download_failure:
        self.failures += 1
    else:
        rename_documents()

def scrape_documents_by_NEC(self, response, n_documents, storage_path):
    driver = response.request.meta["driver"]
    select = Select(driver.find_element(By.NAME, 'searchResult_length'))
    select.select_by_visible_text('100')
    print(f"Downloading {n_documents} documents separately ...")

    checkboxs = driver.find_elements(By.NAME, 'selectCheckBox')
    document_items = driver.find_elements(By.XPATH, '//*[@id="searchResult"]/tbody/tr')
    unzip_dir = None
    existing_names = []
    for i, checkbox in enumerate(checkboxs):
        checkbox.click()
        driver.find_element(By.ID, 'linkDownload').click()
        try:
            unzip_dir = self.unzip_documents(storage_path, wait_unit=0.1, wait_total=10)
            docfile = os.listdir(unzip_dir)[0]

            document_date = document_items[i].find_element(By.XPATH, f'./td[8]').text
            #document_date = re.sub('/', '-', document_date)
            document_type = document_items[i].find_element(By.XPATH, f'./td[3]').text
            document_description = document_items[i].find_element(By.XPATH, f'./td[7]').text
            document_filetype = document_items[i].find_element(By.XPATH, f'./td[12]').text
            document_name = f"date={document_date}&type={document_type}({document_filetype[1:].lower()})&desc={document_description}"
            document_name = replace_invalid_characters(document_name)
            if document_name not in existing_names:
                existing_names.append(document_name)
            else:
                rename_index = 2
                base = document_name
                duplicate = True
                while duplicate:
                    document_name = base + str(rename_index)
                    if document_name not in existing_names:
                        duplicate = False
                        existing_names.append(document_name)
                    else:
                        rename_index += 1
            print(document_name) if PRINT else None
            docfile_extension = docfile.split('.')[-1]
            os.rename(unzip_dir + docfile, f"{storage_path}{document_name}.{docfile_extension.lower()}")
        except FileNotFoundError as error:
            print(f"Downloading {i}/{n_documents} Failed: {error}")
            self.failures += 1
        checkbox.click()
    if unzip_dir is not None:
        os.rmdir(unzip_dir)

def scrape_documents_by_NEC2_USELESS(self, response, n_documents, storage_path):
    driver = response.request.meta["driver"]
    driver.find_element(By.ID, 'selectAll').click()
    driver.find_element(By.ID, 'linkDownload').click()
    print(f"Downloading {n_documents} documents by 1 download ...")
    # process zip
    try:
        unzip_dir = self.unzip_documents(storage_path)
        # Rename downloaded documents:
        docfiles = os.listdir(unzip_dir)

        driver = response.request.meta["driver"]
        select = Select(driver.find_element(By.NAME, 'searchResult_length'))
        select.select_by_visible_text('100')
        # driver.refresh()
        # time.sleep(5)

        document_items = driver.find_elements(By.XPATH, '//*[@id="searchResult"]/tbody/tr')
        # 'Correspondence for discharge of condition 53530 details of methodology and painting scheme - acceptable 17/02/2011 0 0 1.0.0 .msg'
        day = '[0-9]{2}'
        month = '\w*'
        year = '[0-9]{4}'
        pattern_date = f'{day}/{month}/{year}'
        existing_names = []

        for i, document_item in enumerate(document_items):
            document_info = document_item.text
            date_received = re.search(pattern_date, document_info, re.I).group()
            document_info = document_info.split(date_received)[0]
            date_received = re.sub('/', '-', date_received)

            case_number = re.search('\d+', document_info, re.I).group()
            document_info = document_info.split(case_number)
            document_type = document_info[0].strip()
            description = document_info[1].strip()
            docname = f"date={date_received}&type={document_type}&case_num={case_number}&desc={description}"
            if docname not in existing_names:
                existing_names.append(docname)
            else:
                rename_index = 2
                base = docname
                duplicate = True
                while duplicate:
                    docname = base + str(rename_index)
                    if docname not in existing_names:
                        duplicate = False
                        existing_names.append(docname)
                    else:
                        rename_index += 1
            print(i, unzip_dir + docfiles[i], f"{storage_path}{docname+docfiles[i][-4:]}") if PRINT else None
            os.rename(unzip_dir + docfiles[i], f"{storage_path}{docname+docfiles[i][-4:]}")
            """
            date_received = document_item.xpath(f'./td[8]/text()').get()
            date_received = re.sub('/', '-', date_received)
            document_type = document_item.xpath(f'./td[3]/text()').get()
            case_number = document_item.xpath(f'./td[5]/text()').get()
            description = document_item.xpath(f'./td[7]/text()').get()
            docname = f"date={date_received}&type={document_type}&case_num={case_number}&desc={description}"
            os.rename(unzip_dir + docfiles[i], f"{storage_path}{docname+docfiles[-4:]}")
            #"""

        # n_remaining_documents = n_documents - 10
        # if n_remaining_documents > 0:
        #    docfiles = docfiles[10:]
        # """
        os.rmdir(unzip_dir)
    except FileNotFoundError as error:
        print("Downloading Failed:", error)
        self.failures += 1