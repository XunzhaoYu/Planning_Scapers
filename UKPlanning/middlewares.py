# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from tools.utils import get_project_root
from tools.IP_proxy import update_IP_proxy_zip
import time
from pathlib import Path

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class UkplanningSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class UkplanningDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


from importlib import import_module
from scrapy.exceptions import NotConfigured
from scrapy.http import HtmlResponse
from selenium.webdriver.support.ui import WebDriverWait
from scrapy_selenium.http import SeleniumRequest

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
class SeleniumMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        """Initialize the middleware with the crawler settings"""
        driver_name = crawler.settings.get('SELENIUM_DRIVER_NAME')
        driver_executable_path = crawler.settings.get('SELENIUM_DRIVER_EXECUTABLE_PATH')
        browser_executable_path = crawler.settings.get('SELENIUM_BROWSER_EXECUTABLE_PATH')
        command_executor = crawler.settings.get('SELENIUM_COMMAND_EXECUTOR')
        driver_arguments = crawler.settings.get('SELENIUM_DRIVER_ARGUMENTS')

        if driver_name is None:
            raise NotConfigured('SELENIUM_DRIVER_NAME must be set')

        # let's use webdriver-manager when nothing is specified instead | RN just for Chrome
        if (driver_name.lower() != 'chrome') and (driver_executable_path is None and command_executor is None):
            raise NotConfigured('Either SELENIUM_DRIVER_EXECUTABLE_PATH or SELENIUM_COMMAND_EXECUTOR must be set')

        middleware = cls(
            driver_name=driver_name,
            driver_executable_path=driver_executable_path,
            browser_executable_path=browser_executable_path,
            command_executor=command_executor,
            driver_arguments=driver_arguments
        )

        crawler.signals.connect(middleware.spider_closed, signals.spider_closed)
        return middleware


    def __init__(self, driver_name, driver_executable_path,
                 browser_executable_path, command_executor, driver_arguments):
        """Initialize the selenium webdriver

        Parameters
        ----------
        driver_name: str
            The selenium ``WebDriver`` to use
        driver_executable_path: str
            The path of the executable binary of the driver
        driver_arguments: list
            A list of arguments to initialize the driver
        browser_executable_path: str
            The path of the executable binary of the browser
        command_executor: str
            Selenium remote server endpoint
        """

        webdriver_base_path = f'selenium.webdriver.{driver_name}'

        #driver_klass_module = import_module(f'{webdriver_base_path}.webdriver')
        #driver_klass = getattr(driver_klass_module, 'WebDriver')

        driver_options_module = import_module(f'{webdriver_base_path}.options')
        driver_options_klass = getattr(driver_options_module, 'Options')

        driver_options = driver_options_klass()
        if browser_executable_path:
            driver_options.binary_location = browser_executable_path
        for argument in driver_arguments:
            driver_options.add_argument(argument)

        #driver_options.add_argument("start-maximized")  # for reCAPTCHA
        #driver_options.add_experimental_option("excludeSwitches", ["enable-automation"])  # for reCAPTCHA
        #driver_options.add_experimental_option('useAutomationExtension', False)  # for reCAPTCHA
        #driver_options.add_argument('--disable-blink-features=AutomationControlled')  # for reCAPTCHA
        #driver_options.add_argument("--ignore-certificate-errors")

        # for Chrome proxy without authorization
        """
        proxy_host = 'brd.superproxy.io'
        proxy_port = 22225
        proxy_username = 'brd-customer-hl_99055641-zone-datacenter_proxy1'
        proxy_password = '0z20j2ols2j5'
        PROXY = f'http://{proxy_username}:{proxy_password}@{proxy_host}:{proxy_port}'
        driver_options.add_argument(f"--proxy-server={PROXY}")
        #"""
        # for Chrome proxy with authorization: set proxy.zip
        #"""
        self.proxy_path = f"{Path(get_project_root()).parent}/proxy.zip"
        #print(proxy_path)
        #update_IP_proxy_zip(self.proxy_path, session_id=str(time.time()).split('.')[1])
        self.IP_index = 0
        self.IP_address = update_IP_proxy_zip(self.proxy_path, IP_index=self.IP_index)
        driver_options.add_extension(self.proxy_path)
        self.request_counter = 0
        #"""

        # set webdriver, compatible with diverse Selenium versions.
        """ 
        # locally installed driver
        if driver_executable_path is not None:
            driver_kwargs = {
                'executable_path': driver_executable_path,
                f'{driver_name}_options': driver_options
            }
            self.driver = driver_klass(**driver_kwargs)
        # remote driver
        elif command_executor is not None:
            from selenium import webdriver
            capabilities = driver_options.to_capabilities()
            self.driver = webdriver.Remote(command_executor=command_executor, desired_capabilities=capabilities)
        # webdriver-manager
        else:
            # selenium4+
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            if driver_name and driver_name.lower() == 'chrome':
                service = Service()
                self.driver = webdriver.Chrome(service=service, options=driver_options)
        """
        # set webdriver, works with Selenium 4+ and Chrome only
        #from selenium import webdriver
        #from selenium.webdriver.chrome.service import Service
        if driver_name and driver_name.lower() == 'chrome':
            service = Service()
            #self.driver = webdriver.Chrome(service=service, options=driver_options)
            self.driver_options = driver_options
            self.service = service
            self.driver = webdriver.Chrome(service=self.service, options=self.driver_options)

    def reset_driver(self, IP_index=0):
        self.driver.quit()
        #update_IP_proxy_zip(self.proxy_path, session_id=str(time.time()).split('.')[1])
        IP_address = update_IP_proxy_zip(self.proxy_path, IP_index=IP_index)
        self.driver = webdriver.Chrome(service=self.service, options=self.driver_options)
        return IP_address

    def process_request(self, request, spider):
        """Process a request using the selenium driver if applicable"""
        if not isinstance(request, SeleniumRequest):
            return None

        IP_FREQUENCY = 10
        IP_attempts = 0
        while IP_attempts < 10:
            if self.request_counter == IP_FREQUENCY:
                self.IP_index = (self.IP_index + 1)%50
                self.IP_address = self.reset_driver(self.IP_index)
                self.request_counter = 1
            else:
                self.request_counter += 1
            """
                self.driver.get('http://lumtest.com/myip.json')
                ip_info = self.driver.page_source
                ip = ip_info.split('"ip":')[1].split(',')[0]
                country = ip_info.split('"country":')[1].split(',')[0]
                print(f"ip: {ip}.  country: {country}.  request counter: {self.request_counter}")
            """

            try:
                self.driver.get(request.url)
                print(f"IP_index: {self.IP_index}, IP: {self.IP_address}, request counter: {self.request_counter}")
                break
            except:
                IP_attempts += 1
                self.request_counter = IP_FREQUENCY
                print(f"IP_index: {self.IP_index}, IP: {self.IP_address}, request counter: {self.request_counter}, IP connection failed {IP_attempts} times.")

        for cookie_name, cookie_value in request.cookies.items():
            self.driver.add_cookie({'name': cookie_name, 'value': cookie_value})

        if request.wait_until:
            WebDriverWait(self.driver, request.wait_time).until(request.wait_until)
        if request.screenshot:
            request.meta['screenshot'] = self.driver.get_screenshot_as_png()
        if request.script:
            self.driver.execute_script(request.script)

        body = str.encode(self.driver.page_source)

        # Expose the driver via the "meta" attribute
        request.meta.update({'driver': self.driver})

        return HtmlResponse(
            self.driver.current_url,
            body=body,
            encoding='utf-8',
            request=request
        )

    def spider_closed(self):
        """Shutdown the driver when spider is closed"""
        self.driver.quit()


class SeleniumMiddleware_Linux(SeleniumMiddleware):
    def __init__(self, driver_name, driver_executable_path,
                 browser_executable_path, command_executor, driver_arguments):
        """Initialize the selenium webdriver

        Parameters
        ----------
        driver_name: str
            The selenium ``WebDriver`` to use
        driver_executable_path: str
            The path of the executable binary of the driver
        driver_arguments: list
            A list of arguments to initialize the driver
        browser_executable_path: str
            The path of the executable binary of the browser
        command_executor: str
            Selenium remote server endpoint
        """

        webdriver_base_path = f'selenium.webdriver.{driver_name}'

        #driver_klass_module = import_module(f'{webdriver_base_path}.webdriver')
        #driver_klass = getattr(driver_klass_module, 'WebDriver')

        driver_options_module = import_module(f'{webdriver_base_path}.options')
        driver_options_klass = getattr(driver_options_module, 'Options')

        driver_options = driver_options_klass()
        if browser_executable_path:
            driver_options.binary_location = browser_executable_path
        for argument in driver_arguments:
            driver_options.add_argument(argument)

        driver_options.add_argument("start-maximized")  # for reCAPTCHA
        driver_options.add_experimental_option("excludeSwitches", ["enable-automation"])  # for reCAPTCHA
        driver_options.add_experimental_option('useAutomationExtension', False)  # for reCAPTCHA
        driver_options.add_argument('--disable-blink-features=AutomationControlled')  # for reCAPTCHA

        print(' driver_executable_path:--------', driver_executable_path)
        # for AWS EC2
        # driver_options.binary_location = '/home/ec2-user/chrome-linux64/chrome'
        # driver_options.add_argument("--no-sandbox")
        driver_options.add_argument("--disable-dev-shm-usage")

        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        # from webdriver_manager.chrome import ChromeDriverManager  # for AWS EC2
        if driver_name and driver_name.lower() == 'chrome':
            service = Service()
            # self.driver = webdriver.Chrome(service=service, options=driver_options)  # for AWS EC2
            self.driver = webdriver.Chrome(service=service, options=driver_options)
            # self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=driver_options)  # for AWS EC2

            # self.driver.get('https://github.com/')
            # print(self.driver.title)
            #self.driver.get('https://www.google.com/')
            #print(self.driver.title)
            # self.driver.get('https://publicaccess.aberdeencity.gov.uk/online-applications/applicationDetails.do?keyVal=ZZZYA4BZSK176&activeTab=summary/')
            # print(self.driver.title)