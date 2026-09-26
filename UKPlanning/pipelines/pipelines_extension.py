# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import re
from pipelines.pipelines import DownloadFilesPipeline


# https://docs.scrapy.org/en/latest/_modules/scrapy/pipelines/files.html#FilesPipeline.get_media_requests
class DownloadFilesPipeline(DownloadFilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        #print(info.spider.crawler.stats.get_value('file_count'))
        #print(info.spider.crawler.stats.get_value(f"file_status_count/{200}"))
        try:
            string_data = str(response.headers['Content-Disposition'], 'utf-8')
            doc_extension = string_data.split('.')[-1]
            doc_extension = re.sub(r'[^a-zA-Z0-9]', '', doc_extension)
            #print(f"document {info.spider.crawler.stats.get_value('file_count')} extension: {doc_extension}")
            document_name = f"{request.meta.get('document_name')}.{doc_extension}"
            print('pipeline extension:', document_name)
            return document_name
        except AttributeError:
            pass
        return request.meta.get('document_name')
