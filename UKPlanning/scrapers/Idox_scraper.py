import os, re, time, random
import pandas as pd

from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from configs.settings import PRINT
from general.base_scraper import Base_Scraper
from general.document_utils import replace_invalid_characters, get_documents
from general.items import DownloadFilesItem
from general.utils import unique_columns, scrape_data_items, scrape_for_csv, scrape_multi_tables_for_csv


class Idox_Scraper(Base_Scraper):
    """
    新版 Idox 爬虫 (Idox PublicAccess 门户通用爬虫)。
    New Idox scraper, targeting local authorities running the "Idox PublicAccess" planning portal.

    页面结构说明 / Page structure notes
    ------------------------------------------------------------------
    Idox PublicAccess 的申请详情页(applicationDetails.do)分为两类标签:
    Idox's applicationDetails.do page has two kinds of tabs:

    1) "Details" 大标签下的子标签 (Summary / Further Information / Dates / Contacts):
       这些子标签共用同一个 URL (activeTab=summary), 由前端 JS 在同一页面内切换显示内容,
       不会产生新的页面跳转, 因此必须用 driver.find_element(...).click() 来切换,
       并且切换后要用 driver 而不是 response 去读取内容 (response 是切换前的静态快照)。
       These sub-tabs share one URL and are toggled purely by client-side JS, so they
       must be scraped by clicking with Selenium and reading from `driver`, not from the
       (now-stale) initial `response` object.

    2) 顶层标签 (Comments / Constraints / Documents / Related Cases):
       这些标签各自拥有独立的 URL (activeTab=neighbourComments / constraints / documents / relatedCases),
       点击后浏览器会整页跳转。我们通过 CSV 中的 other_fields.comment_url / docs_url 字段验证了这一点
       (同一个 keyVal, 但 activeTab 参数不同)。因此这些标签改用新的 SeleniumRequest 整页请求,
       请求返回后可以放心使用 `response.xpath(...)`，因为此时 response 已经是该标签整页加载后的快照。
       These top-level tabs each live at their own URL (confirmed via the
       other_fields.comment_url / other_fields.docs_url columns in the CSV, which share the same
       keyVal but differ by activeTab). We therefore navigate to them with fresh SeleniumRequests,
       and it is then safe to use `response.xpath(...)` because the response reflects a full page
       load for that tab.

    示例 / Examples:
        auth_id = 31, Blackpool:
            page:   https://idoxpa.blackpool.gov.uk/online-applications/applicationDetails.do?activeTab=summary&keyVal=_BLCKP_DCAPR_23417
            comments: .../applicationDetails.do?activeTab=neighbourComments&keyVal=_BLCKP_DCAPR_23417
            documents: .../applicationDetails.do?activeTab=documents&keyVal=_BLCKP_DCAPR_23417
        auth_id = 34, Bolton (需要先用申请编号搜索, 因为原始 url 会过期/失效):
            search: https://paplanning.bolton.gov.uk/online-applications/search.do?action=simple&searchType=Application
            page:   https://paplanning.bolton.gov.uk/online-applications/applicationDetails.do?activeTab=summary&keyVal=ZZZPEGDEPM788
    """

    name = 'Idox_Scraper'

    # 默认使用项目通用的下载管道 (settings.py 中的 DownloadFilesPipeline)。
    # Use the project's default download pipeline defined in settings.py; no override needed here.
    # custom_settings = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 每个 Base_Scraper 子类都需要在 __init__ 中指定 self.parse_func。
        # Every sub-class of Base_Scraper must define self.parse_func(s) in __init__.
        if self.auth in ['Bolton']:
            # Bolton 的 CSV 起始 url 可能已过期, 需要先按申请编号(uid)搜索, 再跳转到真实详情页。
            # Bolton's stored start url may be stale, so we search by uid first.
            self.url_check = True
            self.url_preprocess = self.url_preprocess_Bolton
        else:
            self.parse_func = self.parse_data_item_Idox

    # ------------------------------------------------------------------
    # 字段映射表 / Field-name mapping dictionaries
    # 这些字典把网页上显示的标签文字 (th 文本) 映射到 app_df 里的标准列名。
    # These dictionaries map the human-readable labels shown on the page (th text)
    # to our standardised app_df column names.
    # ------------------------------------------------------------------

    # Summary 子标签: 10 + 2 个别名 / Summary sub-tab
    summary_dict = {'Reference': 'uid',
                    'Application Reference': 'uid',
                    'Planning Portal Reference': 'other_fields.planning_portal_id',
                    'Alternative Reference': 'altid',

                    'Application Received': 'other_fields.date_received',
                    'Application Received Date': 'other_fields.date_received',
                    'Application Registered': 'other_fields.date_received',
                    'Application Validated': 'other_fields.date_validated',

                    'Address': 'address',
                    'Location': 'address',
                    'Proposal': 'description',
                    'Status': 'other_fields.status',
                    'Decision': 'other_fields.decision',
                    'Decision Issued Date': 'other_fields.decision_issued_date',
                    'Appeal Status': 'other_fields.appeal_status',
                    'Appeal Decision': 'other_fields.appeal_result',
                    'Local Review Body Status': 'other_fields.local_review_body_status',
                    'Local Review Body Decision': 'other_fields.local_review_body_decision',
                    }

    # Further Information 子标签 / Further Information sub-tab
    details_dict = {'Application Type': 'other_fields.application_type',
                    'Decision': 'other_fields.decision',
                    'Actual Decision Level': 'other_fields.actual_decision_level',
                    'Expected Decision Level': 'other_fields.expected_decision_level',
                    'Decision Level': 'other_fields.expected_decision_level',

                    'Case Officer': 'other_fields.case_officer',
                    'Parish': 'other_fields.parish',
                    'Amenity Society': 'other_fields.amenity_society',
                    'Ward': 'other_fields.ward_name',
                    'District Reference': 'other_fields.district',
                    'Applicant Name': 'other_fields.applicant_name',
                    'Applicant Address': 'other_fields.applicant_address',
                    'Agent Name': 'other_fields.agent_name',
                    'Agent Company Name': 'other_fields.agent_company',
                    'Agent Phone Number': 'other_fields.agent_phone',
                    'Agent Address': 'other_fields.agent_address',
                    'Environmental Assessment Requested': 'other_fields.environmental_assessment',
                    'Environmental Assessment Required': 'other_fields.environmental_assessment',
                    'Community Council': 'other_fields.community_council',
                    'Community': 'other_fields.community_council',
                    'Community/Town Council': 'other_fields.community_council',
                    }

    # Important Dates 子标签 (仅保留常用字段, 完整列表见 Idox_scraper_old.py)
    # Important Dates sub-tab (kept concise here; see Idox_scraper_old.py for the exhaustive list)
    dates_dict = {'Application Received Date': 'other_fields.date_received',
                  'Application Validated Date': 'other_fields.date_validated',
                  'Date Application Valid': 'other_fields.date_validated',
                  'Application Valid Date': 'other_fields.date_validated',
                  'Valid Date': 'other_fields.date_validated',
                  'Application Registered Date': 'other_fields.date_validated',

                  'Expiry Date': 'other_fields.application_expires_date',
                  'Application Expiry Date': 'other_fields.application_expires_date',
                  'Statutory Expiry Date': 'other_fields.statutory_expires_date',

                  'Expiry Date for Comment': 'other_fields.comment_expires_date',
                  'Expiry Date for Comments': 'other_fields.comment_expires_date',
                  'Last Date For Comments': 'other_fields.comment_expires_date',
                  'Last Date for Comments': 'other_fields.comment_expires_date',
                  'Closing Date for Comments': 'other_fields.comment_expires_date',

                  'Actual Committee Date': 'other_fields.meeting_date',
                  'Committee Date': 'other_fields.meeting_date',
                  'Date of Committee Meeting': 'other_fields.meeting_date',

                  'Neighbour Consultation Expiry Date': 'other_fields.neighbour_consultation_end_date',
                  'Latest Neighbour Consultation Date': 'other_fields.neighbour_consultation_start_date',

                  'Consultation Expiry Date': 'other_fields.consultation_end_date',
                  'Consultation Deadline': 'other_fields.consultation_end_date',
                  'Consultation End Date': 'other_fields.consultation_end_date',

                  'Target Date': 'other_fields.target_decision_date',
                  'Target Date for Decision': 'other_fields.target_decision_date',
                  'Target Decision Date': 'other_fields.target_decision_date',
                  'Internal Target Date': 'other_fields.target_decision_date',

                  'Decision Made Date': 'other_fields.decision_date',
                  'Decision Date': 'other_fields.decision_date',
                  'Decision Issued Date': 'other_fields.decision_issued_date',
                  'Decision Printed Date': 'other_fields.decision_published_date',
                  'Permission Expiry Date': 'other_fields.permission_expires_date',

                  'Determination Deadline': 'other_fields.determination_date',
                  }

    # ------------------------------------------------------------------
    # 通用工具方法 / Shared helper methods
    # ------------------------------------------------------------------

    def create_item(self, driver, folder_name, file_urls, document_names):
        """
        构建下载文件用的 Item, 并把当前 Selenium session 的 cookies 一并带上,
        供 pipelines.DownloadFilesPipeline 用 Scrapy Request 下载附件时复用登录态。
        Build a DownloadFilesItem carrying the current Selenium session cookies,
        so that pipelines.DownloadFilesPipeline can reuse the session when
        downloading each attachment via a plain Scrapy Request.
        """
        if not os.path.exists(self.failed_downloads_path + folder_name):
            os.mkdir(self.failed_downloads_path + folder_name)

        item = DownloadFilesItem()
        item['file_urls'] = file_urls
        item['document_names'] = document_names

        cookies = driver.get_cookies()
        print(f'cookies:, {cookies}') if PRINT else None
        item['session_cookies'] = cookies
        return item

    @staticmethod
    def is_empty(value):
        """
        判断 app_df 中某个字段是否"尚未填充"。
        Check whether a field in app_df is still unfilled / empty.
        """
        if pd.isnull(value):
            return True
        text = str(value).strip().lower()
        return text in ('', 'nan')

    def scrape_data(self, app_df, items, item_values, dictionary):
        """
        把一组 (th, td) Selenium WebElement 对, 按 dictionary 映射写入 app_df。
        与 CivicaJason_scraper.py / Ocella_scraper.py 中同名逻辑保持一致的风格:
        - 已存在且非"see source"的字段不覆盖 (保留最先抓到的数据源);
        - 字典中没有的新字段动态补列。
        Map a batch of (th, td) Selenium WebElement pairs into app_df using `dictionary`.
        Mirrors the style used in CivicaJason_scraper.py / Ocella_scraper.py:
        - existing, already-filled fields (other than the 'see source' placeholder) are kept;
        - unseen labels are added to app_df as new columns on the fly.
        """
        for item, value in zip(items, item_values):
            item_name = item.get_attribute('innerText').strip()
            data_name = dictionary[item_name] # *** changed.
            item_value = value.get_attribute('innerText').strip()
            try:
                current = app_df.at[data_name]
                if self.is_empty(current) or str(current).strip().lower() == 'see source':
                    app_df.at[data_name] = item_value
                    print(f'    <{item_name}> scraped: {item_value}') if PRINT else None
                else:
                    print(f'    <{item_name}> already filled: {current}') if PRINT else None
            except KeyError:
                app_df[data_name] = item_value
                print(f'    <{item_name}> scraped (new column): {item_value}') if PRINT else None
        return app_df

    # ------------------------------------------------------------------
    # Bolton 专用: 按申请编号搜索 / Bolton-specific: search by application id
    # ------------------------------------------------------------------

    def url_preprocess_Bolton(self, url):
        if url.startswith('https://paplanning.bolton.gov.uk/online-applications/applicationDetails.do?'):
            self.parse_func = self.parse_data_item_Idox
            return url
        else:
            self.parse_func = self.search_by_appID_Idox
            return 'https://paplanning.bolton.gov.uk/online-applications/search.do?action=simple&searchType=Application'

    def search_by_appID_Idox(self, response):
        driver = response.request.meta['driver']
        app_df = response.meta['app_df']
        url = response.request.url
        print(f'search page url: {url}') if PRINT else None

        # 用 uid (申请编号) 搜索, 定位到真实详情页。
        # Use uid (application reference) to search and locate the real detail page.
        app_id = app_df.at['uid']
        input_reference = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//input[@id="simpleSearchString"]')))
        input_reference.click()
        input_reference.send_keys(app_id)

        driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
        for tries in range(3):  # 有时候第一次点击搜索没反应, 重试几次 / retry a few times, search button can be flaky
            time.sleep(random.uniform(1., 1.5))
            try:
                driver.find_element(By.XPATH, '//input[@type="submit"]').click()
            except NoSuchElementException:
                time.sleep(2)

        # 搜索成功后会自动跳转到结果页, 等待跳转完成。
        # A successful search auto-redirects to the results page; wait for that redirect.
        while driver.current_url == url:
            time.sleep(random.uniform(4., 5.))

        # 从结果页里拿到 Summary 子标签的真实链接 (跳转后的临时 url 不能长期复用)。
        # Grab the real 'summary' tab link from the results page (the redirected url itself is transient).
        summary_tab = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="subtab_summary"]')))
        real_url = summary_tab.get_attribute('href')
        app_df.at['url'] = response.urljoin(real_url)
        print(f"correct url: {app_df.at['url']}")

        yield from self.parse_data_item_Idox(response)

    # ------------------------------------------------------------------
    # 主入口: Details 大标签 (Summary / Further Information / Dates)
    # Entry point: the "Details" mega-tab (Summary / Further Information / Dates)
    # ------------------------------------------------------------------

    def parse_data_item_Idox(self, response):
        app_df = response.meta['app_df']
        driver = response.request.meta['driver']
        scraper_name = app_df.at['scraper_name']
        folder_name = self.setup_storage_path(app_df)
        max_file_name_len = self.max_folder_file_name_len - len(folder_name) - 5  # 预留5个字符给后缀, 如 .pdf / reserve 5 chars for suffix e.g. '.pdf'
        print(f'parse_data_item_Idox, scraper name: {scraper_name}, max_file_name_len: {max_file_name_len}.')

        try:
            content = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="pa"]/div[@class="container"]/div[@class="content"]')))
        except TimeoutException:
            # 该申请详情不可查看 (可能已被撤回/限制公开)。
            # Application details are not viewable (may have been withdrawn / restricted).
            note = response.xpath('//*[@id="main-content"]/article/h1/text()').get()
            print('note: ', note)
            return

        tab_container = content.find_element(By.XPATH, "./div[@class='tabcontainer']")

        # --- 1. Summary (默认就是激活状态, 无需点击) / Summary (active by default, no click needed) ---
        items = tab_container.find_elements(By.XPATH, "./table[@id='simpleDetailsTable']/tbody/tr/th")
        item_values = tab_container.find_elements(By.XPATH, "./table[@id='simpleDetailsTable']/tbody/tr/td")
        print(f'\n1. Summary: {len(items)} items.')
        app_df = self.scrape_data(app_df, items, item_values, self.summary_dict)

        # --- 2. Further Information ---
        try:
            driver.find_element(By.XPATH, '//*[@id="subtab_details"]').click()
            tbody = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//table[@id='applicationDetails']/tbody")))
            items = tbody.find_elements(By.XPATH, './tr/th')
            item_values = tbody.find_elements(By.XPATH, './tr/td')
            print(f'\n2. Further Information: {len(items)} items.')
            app_df = self.scrape_data(app_df, items, item_values, self.details_dict)
        except (NoSuchElementException, TimeoutException):
            # 部分门户没有这个子标签。/ Some portals don't expose this sub-tab.
            print('\n2. Further Information: sub-tab not found, skipped.')

        # --- 3. Important Dates (复用同一个表格 id, 内容已被 JS 换成日期数据) ---
        # --- 3. Important Dates (same table id is reused; JS swaps in the dates content) ---
        try:
            driver.find_element(By.XPATH, '//*[@id="subtab_dates"]').click()
            tbody = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//table[@id='simpleDetailsTable']/tbody")))
            items = tbody.find_elements(By.XPATH, './tr/th')
            item_values = tbody.find_elements(By.XPATH, './tr/td')
            print(f'\n3. Important Dates: {len(items)} items.')
            app_df = self.scrape_data(app_df, items, item_values, self.dates_dict)
        except (NoSuchElementException, TimeoutException):
            print('\n3. Important Dates: sub-tab not found, skipped.')

        # --- 4. 剩下的四个标签 (Comments/Constraints/Documents/RelatedCases) 各自独立成页 ---
        # --- 4. the remaining four tabs each live on their own page ---
        # 用 app_df 里已有的 url 拼出下一个标签的 url (与 comment_url / docs_url 列的构造方式一致)。
        # Build the next tab's url from app_df's own url field (same pattern as the
        # other_fields.comment_url / other_fields.docs_url columns already in the CSV).
        base_url = app_df.at['url']
        comments_url = base_url.replace('activeTab=summary', 'activeTab=neighbourComments')
        yield SeleniumRequest(url=comments_url, callback=self.parse_public_comments_item,
                              meta={'app_df': app_df, 'folder_name': folder_name,
                                    'max_file_name_len': max_file_name_len,
                                    'comment_source': [], 'comment_date': [], 'comment_content': []},
                              dont_filter=True)

    # ------------------------------------------------------------------
    # 5. Comments: 公众意见(neighbourComments) + 法定咨询意见(consulteeComments)
    # 5. Comments: public (neighbourComments) + statutory-consultee (consulteeComments) responses
    # ------------------------------------------------------------------

    @staticmethod
    def scrape_comments(comments, comment_source, comment_date, comment_content):
        """
        解析一页评论列表, 抽取来源/日期/正文, 追加进传入的三个 list。
        逻辑沿用旧版 Idox_scraper_old.py 的容错处理 (不同门户的 HTML 细节略有差异)。
        Parse one page of comments, extracting source / date / body text into the
        three accumulator lists passed in. Defensive logic ported from
        Idox_scraper_old.py, since different councils' HTML varies slightly.
        """
        def scrape_source(comment, label_name):
            temp_source = comment.xpath(f'./{label_name}/text()').get()
            temp_source = temp_source.strip() if temp_source else ''
            for subtag in comment.xpath(f'./{label_name}/*'):
                sub_text = subtag.xpath('./text()').get()
                temp_source += sub_text.strip() if sub_text else ''
            return temp_source

        for comment in comments:
            if comment.xpath('./h2').get():
                temp_source = scrape_source(comment, 'h2')
            elif comment.xpath('./h3').get():
                temp_source = scrape_source(comment, 'h3')
            else:
                temp_source = ''

            comment_wraps = comment.xpath('./div')
            if len(comment_wraps) == 0:
                comment_source.append(temp_source)
                comment_date.append('')
                comment_content.append('')
                continue

            for comment_wrap in comment_wraps:
                comment_source.append(temp_source)
                temp_date = ''
                if comment_wrap.xpath('./h3').get():
                    temp_date = comment_wrap.xpath('./h3/text()').get() or ''
                elif comment_wrap.xpath('./h4').get():
                    temp_date = comment_wrap.xpath('./h4/text()').get() or ''
                comment_date.append(re.sub(r'\s+', ' ', temp_date.strip()))

                temp_content = comment_wrap.xpath('./text()').getall()
                temp_content = re.sub(r'\s+', ' ', ' '.join(temp_content)).strip()
                comment_content.append(temp_content)

    def parse_public_comments_item(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        comment_source = response.meta['comment_source']
        comment_date = response.meta['comment_date']
        comment_content = response.meta['comment_content']

        # 首次进入该标签才需要读取汇总统计数字; 翻页请求(下面的 elif 分支)不需要重复统计。
        # Only read the aggregate counters on the first visit to this tab;
        # follow-up pagination requests (see below) skip re-counting.
        if 'first_visit' not in response.meta or response.meta['first_visit']:
            try:
                strs = response.xpath('//*[@id="commentsContainer"]/ul/li[1]/text()').get()
                public_consulted = int(re.search(r'\d+', strs).group())
                strs = response.xpath('//*[@id="commentsContainer"]/ul/li[2]/text()').get()
                public_received = int(re.search(r'\d+', strs).group())
                public_consulted = max(public_consulted, public_received)

                app_df['other_fields.n_comments_public_total_consulted'] = public_consulted
                app_df['other_fields.n_comments_public_received'] = public_received
                if public_received == 0:
                    app_df['other_fields.n_comments_public_objections'] = 0
                    app_df['other_fields.n_comments_public_supporting'] = 0
                else:
                    strs = response.xpath('//*[@id="commentsContainer"]/ul/li[3]/text()').get()
                    app_df['other_fields.n_comments_public_objections'] = int(re.search(r'\d+', strs).group())
                    strs = response.xpath('//*[@id="commentsContainer"]/ul/li[4]/text()').get()
                    app_df['other_fields.n_comments_public_supporting'] = int(re.search(r'\d+', strs).group())
                print(f"\n5. Public comments: consulted={public_consulted}, received={public_received}.") if PRINT else None
            except (TypeError, AttributeError):
                # 该门户没有公众评论页 / this portal has no public-comments page for this application.
                app_df['other_fields.n_comments_public_total_consulted'] = 0
                app_df['other_fields.n_comments_public_received'] = 0
                app_df['other_fields.n_comments_public_objections'] = 0
                app_df['other_fields.n_comments_public_supporting'] = 0
                print('\n5. Public comments: no comments page for this application.') if PRINT else None

        # 抓取当前页的评论正文(如果有的话)。/ Scrape this page's individual comments, if any.
        try:
            comments = response.xpath('//*[@id="comments"]').xpath('./div')
            self.scrape_comments(comments, comment_source, comment_date, comment_content)
        except TypeError:
            pass

        # 翻页: 若有下一页则递归调用自身; 否则进入法定咨询意见标签。
        # Pagination: recurse into the next page if present; otherwise move on to consultee comments.
        next_page_url = response.xpath('//*[@id="commentsListContainer"]').css('a.next::attr(href)').get()
        if next_page_url:
            next_page_url = response.urljoin(next_page_url)
            yield SeleniumRequest(url=next_page_url, callback=self.parse_public_comments_item,
                                  meta={'app_df': app_df, 'folder_name': folder_name,
                                        'max_file_name_len': response.meta['max_file_name_len'],
                                        'comment_source': comment_source, 'comment_date': comment_date,
                                        'comment_content': comment_content, 'first_visit': False},
                                  dont_filter=True)
        else:
            consultee_url = app_df.at['url'].replace('activeTab=summary', 'activeTab=consulteeComments')
            yield SeleniumRequest(url=consultee_url, callback=self.parse_consultee_comments_item,
                                  meta={'app_df': app_df, 'folder_name': folder_name,
                                        'max_file_name_len': response.meta['max_file_name_len'],
                                        'comment_source': comment_source, 'comment_date': comment_date,
                                        'comment_content': comment_content, 'first_visit': True},
                                  dont_filter=True)

    def parse_consultee_comments_item(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']
        comment_source = response.meta['comment_source']
        comment_date = response.meta['comment_date']
        comment_content = response.meta['comment_content']

        if response.meta.get('first_visit', True):
            try:
                strs = response.xpath('//*[@id="commentsContainer"]/ul/li[1]/text()').get()
                app_df['other_fields.n_comments_consultee_total_consulted'] = int(re.search(r'\d+', strs).group())
                strs = response.xpath('//*[@id="commentsContainer"]/ul/li[2]/text()').get()
                app_df['other_fields.n_comments_consultee_responded'] = int(re.search(r'\d+', strs).group())
            except (TypeError, AttributeError):
                app_df['other_fields.n_comments_consultee_total_consulted'] = 0
                app_df['other_fields.n_comments_consultee_responded'] = 0
            print(f"consultee comments: consulted={app_df.at['other_fields.n_comments_consultee_total_consulted']}, "
                  f"responded={app_df.at['other_fields.n_comments_consultee_responded']}.") if PRINT else None

            # 汇总 n_comments (公众 + 法定咨询) / total n_comments (public + statutory consultee)
            app_df.at['other_fields.n_comments'] = (app_df.at['other_fields.n_comments_consultee_responded'] +
                                                     app_df.at['other_fields.n_comments_public_received'])

        try:
            comments = response.xpath('//*[@id="comments"]').xpath('./div')
            self.scrape_comments(comments, comment_source, comment_date, comment_content)
        except TypeError:
            pass

        next_page_url = response.xpath('//*[@id="commentsListContainer"]').css('a.next::attr(href)').get()
        if next_page_url:
            next_page_url = response.urljoin(next_page_url)
            yield SeleniumRequest(url=next_page_url, callback=self.parse_consultee_comments_item,
                                  meta={'app_df': app_df, 'folder_name': folder_name,
                                        'max_file_name_len': response.meta['max_file_name_len'],
                                        'comment_source': comment_source, 'comment_date': comment_date,
                                        'comment_content': comment_content, 'first_visit': False},
                                  dont_filter=True)
            return

        # 评论抓取完毕: 若有内容则落盘保存, 然后进入 Constraints 标签。
        # Comments done: persist to csv if any, then move on to the Constraints tab.
        if comment_source:
            comment_df = pd.DataFrame({'comment_source': comment_source,
                                       'comment_date': comment_date,
                                       'comment_content': comment_content})
            comment_df.to_csv(f"{self.data_storage_path}{folder_name}/comments.csv", index=False)

        constraints_url = app_df.at['url'].replace('activeTab=summary', 'activeTab=constraints')
        yield SeleniumRequest(url=constraints_url, callback=self.parse_constraints_item,
                              meta={'app_df': app_df, 'folder_name': folder_name,
                                    'max_file_name_len': response.meta['max_file_name_len']},
                              dont_filter=True)

    # ------------------------------------------------------------------
    # 6. Constraints (规划限制条件, 例如是否在保护区/洪泛区内等)
    # 6. Constraints (planning constraints, e.g. conservation area / flood zone, etc.)
    # ------------------------------------------------------------------

    def parse_constraints_item(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']

        # 表格首行是表头, 需要跳过。/ The first row is the header row, skip it.
        rows = response.xpath('//*[@id="caseConstraints"]/tbody/tr')[1:]
        n_constraints = len(rows)
        app_df.at['other_fields.n_constraints'] = n_constraints
        print(f'\n6. Constraints: {n_constraints} items.') if PRINT else None

        if n_constraints > 0:
            constraint_df = pd.DataFrame({
                'name': [row.xpath('./td[1]/text()').get() for row in rows],
                'type': [row.xpath('./td[2]/text()').get() for row in rows],
                'status': [row.xpath('./td[3]/text()').get() for row in rows],
            })
            constraint_df.to_csv(f"{self.data_storage_path}{folder_name}/constraints.csv", index=False)

        docs_url = app_df.at['url'].replace('activeTab=summary', 'activeTab=documents')
        app_df.at['other_fields.docs_url'] = docs_url
        yield SeleniumRequest(url=docs_url, callback=self.parse_documents_item,
                              meta={'app_df': app_df, 'folder_name': folder_name,
                                    'max_file_name_len': response.meta['max_file_name_len']},
                              dont_filter=True)

    # ------------------------------------------------------------------
    # 7. Documents (附件下载)
    # 7. Documents (attachment download)
    # ------------------------------------------------------------------

    def parse_documents_item(self, response):
        app_df = response.meta['app_df']
        folder_name = response.meta['folder_name']

        # 复用项目里已有、经过验证的 Idox 原生文档解析工具函数, 避免重复造轮子。
        # (旧版 Idox_scraper_old.py 的 'documents' 分支也是调用这个工具函数。)
        # Reuse the project's existing, already-tested helper for native Idox document
        # listings instead of re-implementing the parsing logic here.
        # (Idox_scraper_old.py's 'documents' branch also delegates to this same helper.)
        file_urls, document_names = get_documents(response, self.data_upload_path, folder_name)
        n_documents = len(file_urls)
        app_df.at['other_fields.n_documents'] = n_documents
        print(f'\n7. Documents: {n_documents} items, folder_name: {folder_name}.') if PRINT else None

        if n_documents > 0:
            driver = response.request.meta['driver']
            item = self.create_item(driver, folder_name, file_urls, document_names)
            yield item

        related_url = app_df.at['url'].replace('activeTab=summary', 'activeTab=relatedCases')
        yield SeleniumRequest(url=related_url, callback=self.parse_related_cases_item,
                              meta={'app_df': app_df, 'folder_name': folder_name},
                              dont_filter=True)

    # ------------------------------------------------------------------
    # 8. Related Cases (用来反查 UPRN / 房产唯一编号)
    # 8. Related Cases (used to look up the UPRN / unique property reference number)
    # ------------------------------------------------------------------

    def parse_related_cases_item(self, response):
        app_df = response.meta['app_df']

        try:
            n_properties = response.xpath('//*[@id="Property"]/h2/span/text()').get()
            if n_properties is None:
                n_properties = response.xpath('//*[@id="Property"]/h3/span/text()').get()
            n_properties = int(re.search(r'\d+', n_properties).group())
        except (TypeError, AttributeError):
            n_properties = 0
        print(f'\n8. Related Cases: {n_properties} linked properties.') if PRINT else None

        if n_properties == 0:
            self.ending(app_df)
            return

        if n_properties == 1:
            property_url = response.xpath('//*[@id="Property"]/ul/li/a/@href').get()
        else:
            # 多个关联房产时, 用地址做模糊匹配, 找出最接近当前申请地址的那一个。
            # When several properties are linked, fuzzy-match against the application's
            # own address to find the closest one.
            import difflib
            properties = response.xpath('//*[@id="Property"]/ul/li')
            property_names = [p.xpath('./a/text()').get().strip() for p in properties]
            try:
                matched = difflib.get_close_matches(app_df.at['address'], property_names, n=1)[0]
                matched_index = property_names.index(matched)
                property_url = response.xpath(f'//*[@id="Property"]/ul/li[{matched_index + 1}]/a/@href').get()
            except IndexError:
                property_url = None

        if property_url is None:
            self.ending(app_df)
            return

        property_url = response.urljoin(property_url)
        yield SeleniumRequest(url=property_url, callback=self.parse_uprn_item, meta={'app_df': app_df})

    def parse_uprn_item(self, response):
        app_df = response.meta['app_df']
        uprn = response.xpath('//*[@id="propertyAddress"]/tbody/tr[1]/td/text()').get()
        if uprn:
            app_df.at['other_fields.uprn'] = uprn.strip()
            print(f"<UPRN> scraped: {app_df.at['other_fields.uprn']}") if PRINT else None
        self.ending(app_df)