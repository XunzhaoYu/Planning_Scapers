# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class UkplanningItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class DownloadFilesItem(scrapy.Item):
    # define the fields for your item here like:
    file_urls = scrapy.Field()
    files = scrapy.Field()
    document_names = scrapy.Field()
    session_cookies = scrapy.Field()
    #session_csrf = scrapy.Field()
    payloads = scrapy.Field()
    IP_index = scrapy.Field()

    def __repr__(self):
        """print out nothing"""
        return repr({})