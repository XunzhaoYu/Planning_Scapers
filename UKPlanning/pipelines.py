# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.files import FilesPipeline

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class UkplanningPipeline:
    def process_item(self, item, spider):
        return item


class DownloadFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None):
        origin_name = request.url.split('/')[-1]
        # Example: csv?auth=Barnet&no_kin=0&pg_sz=500&page=1&start_date=2015-03-01&end_date=2015-03-31&compress=on
        strs = origin_name.split('&')
        # Example: csv?auth=Barnet    no_kin=0    pg_sz=500   page=1  start_date=2015-03-01   end_date=2015-03-31     compress=on
        auth = strs[0].split('=')[-1]
        file_name: str = f"{auth}/{strs[4]}&{strs[5]}-{4-int(strs[3][-1])}.csv"
        return file_name