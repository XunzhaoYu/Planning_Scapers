# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.files import FilesPipeline
from scrapy import Request
import json, time, requests

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class UkplanningPipeline:
    def process_item(self, item, spider):
        return item


class DownloadFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split('/')[-1]
        #print("downloading...", request.url.split('/')[-1])
        """
        origin_name = request.url.split('/')[-1]
        # Example: csv?auth=Barnet&no_kin=0&pg_sz=500&page=1&start_date=2015-03-01&end_date=2015-03-31&compress=on
        strs = origin_name.split('&')
        # Example: csv?auth=Barnet    no_kin=0    pg_sz=500   page=1  start_date=2015-03-01   end_date=2015-03-31     compress=on
        auth = strs[0].split('=')[-1]
        file_name: str = f"{auth}/{strs[4]}&{strs[5]}-{4-int(strs[3][-1])}.csv"
        """
        return file_name

    def get_media_requests(self, item, info):
        file_urls = item.get(self.FILES_URLS_FIELD)
        try:
            #csrf = item.get('session_csrf')
            cookie = item.get('session_cookie')
        except TypeError:
            #csrf = None
            cookie = None

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
        for file_url in file_urls:
            #payload = csrf
            #requests.append(Request(file_url, headers=header, method="POST", body=json.dumps(payload)))
            requests.append(Request(file_url, method="GET", cookies=cookie))
        return requests
