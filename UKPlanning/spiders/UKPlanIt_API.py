import csv
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from items import DownloadFilesItem
from tools.utils import get_pages


def load_auths(src_path='scraper_name.csv'):
    """
    Use the following request to access a csv file of existing scrapers and the range of available applications:
    https://www.planit.org.uk/api/areas/csv?pg_sz=500&area_type=planning&select=scraper_name,min_date,max_date,total&sort=scraper_name
    """
    rows = []
    scrapers = []
    start_years = []
    start_months = []
    with open(src_path, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        for row in spamreader:
            rows.append(row[:2])  # we need only scraper_name, start_date, end_date
    for row in rows[1:-1]:  # the first row is header, the last row is blank.
        scrapers.append(row[0])
        start_years.append(row[1][:4])
        start_months.append(row[1][5:7])
    return scrapers, start_years, start_months


def load_start_end_dates(year, month):
    # designed for monthly
    end_dates = ['31', '28', '31', '30', '31', '30', '31', '31', '30', '31', '30', '31']
    end_dates2 = ['31', '29', '31', '30', '31', '30', '31', '31', '30', '31', '30', '31']

    year_month = '{:s}-{:s}-'.format(str(year), str(month + 1).zfill(2))
    start_date = year_month + '01'
    if year % 4 != 0:
        end_date = year_month + end_dates[month]
    else:
        end_date = year_month + end_dates2[month]
    return start_date, end_date


class UKPlanIt_API_Scraper(CrawlSpider):
    name = 'UKPlanIt_API'
    allowed_domains = ['www.planit.org.uk']
    start_urls = ['https://www.planit.org.uk/']

    rules = (
        Rule(LinkExtractor(allow=r'api/applics/'),
             callback='parse_item',
             follow=True),
    )

    def parse_item(self, response):
        auths, start_years, start_months = load_auths()
        end_year = 2024
        file_urls = []
        for index in range(int(self.start_index), int(self.end_index)):
            auth = auths[index]
            start_year = int(start_years[index])
            start_month = int(start_months[index])
            print(auth, start_year, start_month)
            n_page = get_pages(auth)
            n_page_size = 1000 if n_page == 1 else 600

            for month in range(start_month-1, 12):
                start_date, end_date = load_start_end_dates(start_year, month)
                for page in range(n_page):
                    file_url = "csv?auth={:s}&no_kin=0&pg_sz={:d}&page={:d}&start_date={:s}&end_date={:s}&compress=on".format(auth, n_page_size, page+1, start_date, end_date)
                    file_url = response.urljoin(file_url)
                    file_urls.append(file_url)
            for year in range(start_year+1, end_year):
                for month in range(12):
                    start_date, end_date = load_start_end_dates(year, month)
                    for page in range(n_page):
                        file_url = "csv?auth={:s}&no_kin=0&pg_sz={:d}&page={:d}&start_date={:s}&end_date={:s}&compress=on".format(auth, n_page_size, page+1, start_date, end_date)
                        file_url = response.urljoin(file_url)
                        file_urls.append(file_url)

        item = DownloadFilesItem()
        item['file_urls'] = file_urls
        print(f"Number of csv files to access: {len(file_urls)}")
        yield item

