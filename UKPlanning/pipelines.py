# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.files import FilesPipeline
from scrapy import Request
from tools.curl import upload_file
from tools.utils import get_data_storage_path
from settings import CLOUD_MODE
import os, logging
"""
import json, time, requests
from io import BytesIO
from scrapy.utils.misc import md5sum
from contextlib import suppress

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
"""

class UkplanningPipeline:
    def process_item(self, item, spider):
        return item

# https://docs.scrapy.org/en/latest/_modules/scrapy/pipelines/files.html#FilesPipeline.get_media_requests
class DownloadFilesPipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        file_urls = item.get(self.FILES_URLS_FIELD)
        document_names = item.get('document_names')
        # csrf = item.get('session_csrf')
        cookies = item.get('session_cookies')

        requests = []
        """
        header = {  # POST /online-applications/download/ HTTP/1.1,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8,zh-CN;q=0.7,zh;q=0.6,zh-TW;q=0.5',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            #'Content-Length': '156',
            #'Content-Type': 'application/x-www-form-urlencoded',
            'Cookie': f'JSESSIONID={cookie}' ,   #-BJs9qDBv54Sla-_l8QXKRjyGqlRBdd3Id4eRveA.npaweb',
            'Host': 'planapps-online.ne-derbyshire.gov.uk',
            #'Origin': 'https://planapps-online.ne-derbyshire.gov.uk',
            'Referer': 'https://planapps-online.ne-derbyshire.gov.uk/online-applications/applicationDetails.do?activeTab=documents&keyVal=Q3QPGOLI06K00',
            'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'sec-Ch-Ua-Mobile': '?0',
            'sec-Ch-Ua-Platform': 'macOS',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        }
        #"""
        for index in range(len(file_urls)):
            # payload = csrf
            # requests.append(Request(file_urls[index], headers=header, method="POST", body=json.dumps(payload)))
            requests.append(Request(file_urls[index], method="GET", cookies=cookies, meta={'document_name': document_names[index]}))
        return requests

    def file_path(self, request, response=None, info=None, *, item=None):
        #print(info.spider.crawler.stats.get_value('file_count'))
        #print(info.spider.crawler.stats.get_value(f"file_status_count/{200}"))
        return request.meta.get('document_name')

# def file_downloaded(self, response, request, info, *, item=None):


    def item_completed(self, results, item, info):
        DOWNLOAD_COMPLETED = True
        storage_path = get_data_storage_path()
        if CLOUD_MODE:  # For AWS EC2 instances.
            for success, file_info_or_error in results:
                if success:  # download succeeded, upload to cloud.
                    file_path = file_info_or_error['path']

                    auth = file_path.split('-')[0]
                    if upload_file(auth + '/' + file_path) == 0:  # upload succeeded, delete local file.
                    #if upload_file(file_path) == 0:  # upload succeeded, delete local file.
                        os.remove(storage_path + file_path)
                else:  # download failed, record logs.
                    DOWNLOAD_COMPLETED = False
                    logging.error(msg=file_info_or_error)
        else:  # For local machines.
            for success, file_info_or_error in results:
                if not success:
                    DOWNLOAD_COMPLETED = False
                    logging.error(msg=file_info_or_error)

        # if all documents have been downloaded.
        if DOWNLOAD_COMPLETED:
            folder_name = results[0][1]['path'].split('/')[0]
            failed_downloads_path = f"{storage_path}failed_downloads/{folder_name}"
            os.rmdir(failed_downloads_path)

        if CLOUD_MODE:
            n_documents = len(results)
            for i in range(n_documents):
                try:  # scanning downloaded documents to get authority name and folder name.
                    folder_name = results[i][1]['path'].split('/')[0]
                    folder_path = storage_path + folder_name
                    if not os.listdir(folder_path):  # if all files in this application folder have been uploaded, delete the empty folder.
                        os.rmdir(folder_path)
                    else:  # if not empty, move the folder to failed_upload_path.
                        failed_upload_path = f"{storage_path}failed_uploads/{folder_name}"
                        os.mkdir(failed_upload_path)
                        for filename in os.listdir(folder_path):
                            os.rename(f"{folder_path}/{filename}", f"{failed_upload_path}/{filename}")
                        assert not os.listdir(folder_path)
                        os.rmdir(folder_path)
                    break
                except TypeError:
                    pass
        return super().item_completed(results, item, info)

