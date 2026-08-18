import os, re, time, random
import pandas as pd
import difflib

from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from configs.settings import PRINT
from general.base_scraper import Base_Scraper
from general.document_utils import replace_invalid_characters, get_documents
from general.items import DownloadFilesItem
from general.utils import unique_columns, scrape_data_items, scrape_for_csv, scrape_multi_tables_for_csv, is_empty, convert_date


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
        auth_id = 32, Blackpool:
            page:   https://idoxpa.blackpool.gov.uk/online-applications/applicationDetails.do?activeTab=summary&keyVal=_BLCKP_DCAPR_23417
            comments: .../applicationDetails.do?activeTab=neighbourComments&keyVal=_BLCKP_DCAPR_23417
            documents: .../applicationDetails.do?activeTab=documents&keyVal=_BLCKP_DCAPR_23417
        auth_id = 35, Bolton (需要先用申请编号搜索, 因为原始 url 会过期/失效):
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
    # 10 + 2*
    summary_dict = {'Reference': 'uid',  # Non-Empty
                    'Application Reference': 'uid',  # New Duplicate [Derby]
                    'Planning Portal Reference': 'other_fields.planning_portal_id',  # New [Derby]
                    'Alternative Reference': 'altid',
                    #
                    'Application Received': 'other_fields.date_received',
                    'Application Received Date': 'other_fields.date_received',  # New Duplicate [Chelmsford]
                    'Application Registered': 'other_fields.date_received',  # New Duplicate [Rhondda]
                    'Application Validated': 'other_fields.date_validated',
                    #
                    'Address': 'address',
                    'Location': 'address',  # Duplicate [Derby]
                    'Proposal': 'description',
                    'Status': 'other_fields.status',
                    'Decision': 'other_fields.decision',
                    'Decision Issued Date': 'other_fields.decision_issued_date',
                    'Appeal Status': 'other_fields.appeal_status',
                    'Appeal Decision': 'other_fields.appeal_result',
                    'Local Review Body Status': 'other_fields.local_review_body_status',  # New*
                    'Local Review Body Decision': 'other_fields.local_review_body_decision'  # New*
                    }

    # Further Information 子标签 / Further Information sub-tab
    # 10 + 3 + 2*
    details_dict = {'Application Type': 'other_fields.application_type',
                    'Decision': 'other_fields.decision',  # Duplicated in summary
                    'Actual Decision Level': 'other_fields.actual_decision_level',  # New
                    'Expected Decision Level': 'other_fields.expected_decision_level',  # New
                    'Decision Level': 'other_fields.expected_decision_level',  # New Duplicate [Moray]
                    #
                    'Case Officer': 'other_fields.case_officer',
                    'Parish': 'other_fields.parish',
                    'Amenity Society': 'other_fields.amenity_society',  # New [Westminster]
                    'Ward': 'other_fields.ward_name',
                    'District Reference': 'other_fields.district',
                    'Applicant Name': 'other_fields.applicant_name',
                    'Applicant Address': 'other_fields.applicant_address',
                    'Agent Name': 'other_fields.agent_name',
                    'Agent Company Name': 'other_fields.agent_company',
                    'Agent Phone Number': 'other_fields.agent_phone',  # New*
                    'Agent Address': 'other_fields.agent_address',
                    'Environmental Assessment Requested': 'other_fields.environmental_assessment',  # New
                    'Environmental Assessment Required': 'other_fields.environmental_assessment',
                    # New Duplicate [Perth]
                    'Community Council': 'other_fields.community_council',  # New*
                    'Community': 'other_fields.community_council',  # New* Duplicate [BreconBeacons]
                    'Community/Town Council': 'other_fields.community_council',  # New* Duplicate [Caerphilly]
                    }

    # Important Dates 子标签 (仅保留常用字段, 完整列表见 Idox_scraper_old.py)
    # Important Dates sub-tab (kept concise here; see Idox_scraper_old.py for the exhaustive list)
    # 14 + 4 + 1*
    dates_dict = {'Application Received Date': 'other_fields.date_received',  # Duplicated in summary
                  'Application Validated Date': 'other_fields.date_validated',  # Duplicated in summary
                  'Date Application Valid': 'other_fields.date_validated',
                  # Duplicated in summary [NewcastleUnderLyme]
                  'Application Valid Date': 'other_fields.date_validated',  # Duplicated in summary [Oadby]
                  'Valid Date': 'other_fields.date_validated',  # New Duplicated in summary [EastHampshire]
                  'Application Registered Date': 'other_fields.date_validated',
                  # New Duplicated in summary [Hammersmith]

                  'Expiry Date': 'other_fields.application_expires_date',
                  'Application Expiry Date': 'other_fields.application_expires_date',
                  # New Duplicate [MiltonKeynes]
                  'Application Expiry Deadline': 'other_fields.application_expires_date',  # New Duplicate [Sefton]

                  'Statutory Expiry Date': 'other_fields.statutory_expires_date',  # New []
                  #
                  'Expiry Date for Comment': 'other_fields.comment_expires_date',  # New
                  'Expiry Date for Comments': 'other_fields.comment_expires_date',  # New Duplicate [Moray]
                  'Last Date For Comments': 'other_fields.comment_expires_date',  # New Duplicate [Edinburgh]
                  'Last Date for Comments': 'other_fields.comment_expires_date',  # New Duplicate [Glasgow]
                  'Last date for public comments': 'other_fields.comment_expires_date',  # New Duplicate [Perth]
                  'Comments To Be Submitted By': 'other_fields.comment_expires_date',  # New Duplicate [Leeds]
                  'Closing Date for Comments': 'other_fields.comment_expires_date',  # New Duplicate [Hammersmith]
                  #
                  'Actual Committee Date': 'other_fields.meeting_date',
                  'Committee Date': 'other_fields.meeting_date',  # New Duplicate [Chelmsford]
                  'Actual Committee or Panel Date': 'other_fields.meeting_date',  # New Duplicate [Gedling]
                  'Date of Committee Meeting': 'other_fields.meeting_date',  # New Duplicate [IOW]
                  'Committee/Delegated List Date': 'other_fields.meeting_date',  # New Duplicate [WestLothian]
                  # Neighbour Consultation Date
                  'Latest Neighbour Consultation Date': 'other_fields.neighbour_consultation_start_date',
                  'Neighbours Last Notified': 'other_fields.neighbour_last_notified_date',
                  # New [NewcastleUnderLyme]
                  'Last Date for Neighbours Responses': 'other_fields.last_neighbour_responses_date',
                  # New [NewcastleUnderLyme]
                  # Neighbour Consultation Expiry
                  'Neighbour Consultation Expiry Date': 'other_fields.neighbour_consultation_end_date',
                  'Neighbour Comments should be submitted by Date': 'other_fields.neighbour_consultation_end_date',
                  # New Duplicate [Bedford]
                  'Neighbour Notification Expiry Date': 'other_fields.neighbour_notification_expiry_date',
                  # New [Sefton]
                  # Consultee Consultation Date
                  'Latest Statutory Consultee Consultation Date': 'other_fields.latest_consultee_consultation_date',
                  # New [Bedford]
                  'Statutory Consultee Consultation Expiry Date': 'other_fields.consultee_consultation_expiry_date',
                  # New [Bedford]
                  # Consultation Expiry
                  'Standard Consultation Date': 'other_fields.standard_consultation_start_date',
                  # *** changed from consultation_start to standard_cosultation_start
                  'Standard Consultation Expiry Date': 'other_fields.standard_consultation_end_date',
                  # *** changed from consultation_end to standard_cosultation_end

                  'Consultation Expiry Date': 'other_fields.consultation_end_date',  # New Duplicate [Chelmsford]
                  'Consultation Deadline': 'other_fields.consultation_end_date',  # New Duplicate [NorthSomerest]
                  'Consultation Period To End On': 'other_fields.consultation_end_date',  # New Duplicate [Torbay]
                  'Consultation End Date': 'other_fields.consultation_end_date',  # New Duplicate [TowerHamlets]

                  'Public Consultation Expiry Date': 'other_fields.public_consultation_end_date',
                  # New Duplicate [Oadby*** changed from consultation_end to public_xxx]
                  'Public Consultation End Date': 'other_fields.public_consultation_end_date',
                  # New Duplicate [IOW]
                  'Public Consultation Ends': 'other_fields.public_consultation_end_date',
                  # New Duplicate [Teignbridge]

                  'Overall Consultation Expiry Date': 'other_fields.overall_consultation_expires_date',  # New []
                  'Overall Date of Consultation Expiry': 'other_fields.overall_consultation_expires_date',
                  # New Duplicate []
                  # Advertisement
                  'Last Advertised In Press Date': 'other_fields.last_advertised_date',
                  'Advertised in Press Date': 'other_fields.last_advertised_date',  # New Duplicate [Glasgow]
                  'Latest Advertisement Expiry Date': 'other_fields.latest_advertisement_expiry_date',
                  'Advertisement Expiry Date': 'other_fields.latest_advertisement_expiry_date',
                  # New Duplicate [NorthHertfordshire]
                  # Site Notice
                  'Last Site Notice Posted Date': 'other_fields.site_notice_start_date',
                  'Latest Site Notice Expiry Date': 'other_fields.site_notice_end_date',
                  'Site Notice Expiry Date': 'other_fields.site_notice_end_date',
                  # New Duplicate [NorthHertfordshire]
                  # Target Date
                  'Internal Target Date': 'other_fields.target_decision_date',
                  'Target Date': 'other_fields.target_decision_date',  # New Duplicate [Bedford]
                  'Target Date for Decision': 'other_fields.target_decision_date',  # New Duplicate [Glasgow]
                  'Target Decision Date': 'other_fields.target_decision_date',  # New Duplicate [Stroud]

                  'Revised Target Date for Decision': 'other_fields.revised_target_decision_date',  # New [Glasgow]
                  'Revised Target Decision Date': 'other_fields.revised_target_decision_date',
                  # New Duplicate [Stroud]

                  'Agreed Extended Target Date': 'other_fields.agreed_extended_target_date',  # New [Teignbridge]
                  'Agreed Extended Date for Decision': 'other_fields.agreed_extended_decision_date',  # New [IOW]
                  # Decision Date
                  'Decision Made Date': 'other_fields.decision_date',
                  'Decision Date': 'other_fields.decision_date',  # Duplicated [Hammersmith]
                  'Decision Issued Date': 'other_fields.decision_issued_date',  # Duplicated in summary

                  'Decision Notice Date': 'other_fields.decision_notice_date',  # New [NewcastleUnderLyme]
                  'Statutory Decision Date': 'other_fields.statutory_decision_date',  # New [IOW]
                  'Earliest Decision Date': 'other_fields.earliest_decision_date',  # New [NewcastleUnderLyme]
                  'Agreed Expiry Date': 'other_fields.agreed_expires_date',  # New
                  'Permission Expiry Date': 'other_fields.permission_expires_date',

                  'Decision Printed Date': 'other_fields.decision_published_date',
                  'Decision Due Date': 'other_fields.decision_due_date',  # New [Chelmsford]
                  'Environmental Impact Assessment Received': 'other_fields.environmental_assessment_date',  # New
                  # Determination
                  'Determination Deadline': 'other_fields.determination_date',  # New
                  'Statutory Determination Deadline': 'other_fields.statutory_determination_deadline',  # New []
                  'Statutory Determination Date': 'other_fields.statutory_determination_deadline',
                  # New Duplicate [Oadby]
                  'Statutory Determination Deadline (Unless there is an Agreed extension date above)': 'other_fields.statutory_determination_deadline',
                  # New Duplicate [Bedford]
                  'Extended Determination Deadline': 'other_fields.extended_determination_deadline',
                  # New [NorthSomerest]
                  'Agreed Extension to Statutory Determination Deadline': 'other_fields.extended_determination_deadline',
                  # New Duplicate [Bedford]

                  'Temporary Permission Expiry Date': 'other_fields.temporary_permission_expires_date',  # New
                  'Local Review Body Decision Date': 'other_fields.local_review_body_decision_date'  # New*
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
        os.makedirs(self.failed_downloads_path + folder_name, exist_ok=True)

        item = DownloadFilesItem()
        item['file_urls'] = file_urls
        item['document_names'] = document_names

        cookies = driver.get_cookies()
        print(f'cookies:, {cookies}') if PRINT else None
        item['session_cookies'] = cookies
        return item

    def scrape_data(self, app_df, items, item_values, dictionary):
        """
        把一组 (th, td) Selenium WebElement 对, 按 dictionary 映射写入 app_df。
        - 已存在且非"see source"的字段不覆盖 (保留最先抓到的数据源);
        - 字典中没有的新字段动态补列。
        Map a batch of (th, td) Selenium WebElement pairs into app_df using `dictionary`.
        - existing, already-filled fields (other than the 'see source' placeholder) are kept;
        - unseen labels are added to app_df as new columns on the fly.
        """
        for item, value in zip(items, item_values):
            item_name = item.get_attribute('innerText').strip()
            data_name = dictionary[item_name] # *** changed.
            item_value = value.get_attribute('innerText').strip()
            try:
                current = app_df.at[data_name]
                if is_empty(current) or str(current).strip().lower() == 'see source':
                    app_df.at[data_name] = convert_date(item_value)
                    print(f'    <{item_name}> scraped: {app_df.at[data_name]}') if PRINT else None
                else:
                    print(f'    <{item_name}> already filled: {current}') if PRINT else None
            except KeyError:
                app_df[data_name] = convert_date(item_value)
                print(f'    <{item_name}> scraped (new column): {app_df.at[data_name]}') if PRINT else None
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
        """
        tab_container = content.find_element(By.XPATH, "./div[@class='tabcontainer']")

        # --- 1. Summary --- 默认就是激活状态, 无需点击 / active by default, no click needed
        items = tab_container.find_elements(By.XPATH, "./table[@id='simpleDetailsTable']/tbody/tr/th")
        item_values = tab_container.find_elements(By.XPATH, "./table[@id='simpleDetailsTable']/tbody/tr/td")
        print(f'\n1. Summary: {len(items)} items.')
        app_df = self.scrape_data(app_df, items, item_values, self.summary_dict)

        # --- 2. Further Information ---
        try:
            driver.find_element(By.XPATH, '//*[@id="subtab_details"]').click()
            tbody = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table[@id='applicationDetails']/tbody")))
            items = tbody.find_elements(By.XPATH, './tr/th')
            item_values = tbody.find_elements(By.XPATH, './tr/td')
            print(f'\n2. Further Information: {len(items)} items.')
            app_df = self.scrape_data(app_df, items, item_values, self.details_dict)
        except (NoSuchElementException, TimeoutException):
            # 部分门户没有这个子标签。/ Some portals don't expose this sub-tab.
            print('\n2. Further Information: sub-tab not found, skipped.')

        # --- 3. Important Dates --- 复用同一个表格 id, 内容已被 JS 换成日期数据 / same table id is reused; JS swaps in the dates content
        try:
            driver.find_element(By.XPATH, '//*[@id="subtab_dates"]').click()
            tbody = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table[@id='simpleDetailsTable']/tbody")))
            items = tbody.find_elements(By.XPATH, './tr/th')
            item_values = tbody.find_elements(By.XPATH, './tr/td')
            print(f'\n3. Important Dates: {len(items)} items.')
            app_df = self.scrape_data(app_df, items, item_values, self.dates_dict)
        except (NoSuchElementException, TimeoutException):
            print('\n3. Important Dates: sub-tab not found, skipped.')
        #"""
        # --- 4. Contacts ---
        def scrape_contacts():
            tabcontainer = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[@class='tabcontainer']")))
            categories = tabcontainer.find_elements(By.XPATH, './div')
            # 每个联系人一行: [category, name, detail1, detail2, ...]，各行长度可以不同，
            # 最后统一补齐成矩形表格。这样不需要在爬取过程中动态维护/补齐二维矩阵。
            # Each contact is one row: [category, name, detail1, detail2, ...].
            # Rows may have different lengths; we pad them to a rectangle only once,
            # at the very end — no need to grow/pad a 2D matrix while scraping.
            rows = []
            for category in categories:
                category_name = category.find_element(By.XPATH, './h3').get_attribute('innerText').strip()
                contact_names = category.find_elements(By.XPATH, './p')  # 每个 <p> 对应一个联系人姓名

                for i, name_selector in enumerate(contact_names, start=1):
                    contact_name = name_selector.get_attribute('innerText').strip()
                    if contact_name is None:
                        # 部分门户网站在联系人 tab 上有 bug，跳过异常项
                        # Some portals have bugs on the contacts tab; skip broken entries.
                        continue

                    # 假定第 i 个 <p>(姓名) 对应第 i 个 <table>(联系方式明细)
                    # Assume the i-th <p> (name) corresponds to the i-th <table> (contact details)
                    detail_rows = category.find_elements(By.XPATH, f'./table[{i}]/tbody/tr')
                    details = [f"{tr.find_element(By.XPATH, './th').get_attribute('innerText').strip()}: {tr.find_element(By.XPATH, './td').get_attribute('innerText').strip()}" for tr in detail_rows]
                    rows.append([category_name, contact_name] + details)

            if rows:
                print(f'    {rows}')
                max_contacts = max(len(r) - 2 for r in rows)  # 减去 category、name 这两列
                #print(f"max number of contact details: {max_contacts}.") if PRINT else None
                columns = ['category', 'name'] + [f'contact{i + 1}' for i in range(max_contacts)]
                padded_rows = [r + [''] * (max_contacts - (len(r) - 2)) for r in rows]

                contact_df = pd.DataFrame(padded_rows, columns=columns)
                contact_df.to_csv(f"{folder_path}contacts.csv", index=False)
                #self.upload_and_delete(folder_name=folder_name, file_name='contacts.csv') if CLOUD_MODE else None

        folder_path = f"{self.data_storage_path}{folder_name}/"
        print(f'    folder path: {folder_path}') if PRINT else None
        try:
            driver.find_element(By.XPATH, '//*[@id="subtab_contacts"]').click()
            print(f'\n4. Contacts.')
            scrape_contacts()
        except (NoSuchElementException, TimeoutException):
            print('\n4. Contacts: sub-tab not found, skipped.')

        # ------------------------------------------------------------------
        # 5. Comments: 公众意见(neighbourComments) + 法定咨询意见(consulteeComments)
        # 5. Comments: public (neighbourComments) + statutory-consultee (consulteeComments) responses
        # ------------------------------------------------------------------
        def scrape_comments(comment_source, comment_date, comment_content):
            comments = driver.find_elements(By.XPATH, '//*[@id="comments"]/div')
            for i, comment in enumerate(comments, start=1):
                temp_source = comment.find_element(By.XPATH, './h2 | ./h3').get_attribute('innerText').strip()
                comment_wraps = comment.find_elements(By.XPATH, './div')
                if len(comment_wraps) == 0:
                    comment_source.append(temp_source)
                    comment_date.append('')
                    comment_content.append('')
                else:
                    # 每个comment source可能留下多条评论内容(算作一条评论)
                    # Each comment source could have multiple comment contents (treated as a single comment)
                    # example: https://planning.n-somerset.gov.uk/online-applications/applicationDetails.do?activeTab=neighbourComments&keyVal=QMES8DLPFI100
                    for comment_wrap in comment_wraps:
                        comment_source.append(temp_source)
                        print(f'\n  --- --- --- comment --- --- --- ')
                        temp_date = comment_wrap.find_element(By.XPATH, './h3 | ./h4').get_attribute('innerText').strip()
                        temp_date2 = re.sub("\s+", " ", temp_date)
                        print(f'    source: {temp_source}, date: {temp_date}, date2: {temp_date2}')
                        comment_date.append(temp_date2)

                        temp_content = comment_wrap.get_attribute('innerText').strip()
                        #print(f'\n  --- --- --- content1 --- --- --- ')
                        #print(temp_content)
                        temp_content = re.sub(temp_date, " ", temp_content)
                        #print(f'\n  --- --- --- content2 (without comment date) --- --- --- ')
                        #print(temp_content2)
                        temp_content2 = re.sub("\s+", " ", temp_content)
                        print(f'\n  --- --- --- content2 (delete spaces and newlines) --- --- --- ')
                        print(temp_content2)
                        comment_content.append(temp_content2)

        def scrape_comments_old(comments, comment_source, comment_date, comment_content):
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
                    print(
                        f"\n5. Public comments: consulted={public_consulted}, received={public_received}.") if PRINT else None
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
                print(
                    f"consultee comments: consulted={app_df.at['other_fields.n_comments_consultee_total_consulted']}, "
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

        comment_source, comment_date, comment_content = [], [], []
        try:
            driver.find_element(By.XPATH, '//*[@id="tab_neighbourComments"]').click()
            summary_stats = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="commentsContainer"]/ul')))
            summary_stats = summary_stats.find_elements(By.XPATH, './li')[:4]
            summary_stat_strs = [stat.get_attribute('innerText').strip() for stat in summary_stats]
            summary_stat_nums = [int(re.search(r"\d+", stat_str).group()) for stat_str in summary_stat_strs]
            print(f"\n5.1. Public Comments: Consulted {summary_stat_nums[0]}, Received {summary_stat_nums[1]}, "
                  f"Objections {summary_stat_nums[2]}, Supporting {summary_stat_nums[3]}.")
            app_df['other_fields.n_comments_public_total_consulted'] = summary_stat_nums[0]
            app_df['other_fields.n_comments_public_received'] = summary_stat_nums[1]
            app_df['other_fields.n_comments_public_objections'] = summary_stat_nums[2]
            app_df['other_fields.n_comments_public_supporting'] = summary_stat_nums[3]

            if app_df['other_fields.n_comments_public_received'] > 0:
                scrape_comments(comment_source, comment_date, comment_content)
        except (NoSuchElementException, TimeoutException):
            print('\n5. Comments: sub-tab not found, skipped.')
            # Public Comments
            app_df['other_fields.n_comments_public_total_consulted'] = 0
            app_df['other_fields.n_comments_public_received'] = 0
            app_df['other_fields.n_comments_public_objections'] = 0
            app_df['other_fields.n_comments_public_supporting'] = 0
            # Consultee Comments
            app_df['other_fields.n_comments_consultee_total_consulted'] = 0
            app_df['other_fields.n_comments_consultee_responded'] = 0
            # Total
            app_df.at['other_fields.n_comments'] = 0

        # If public comments page exists, continue for consultee comments:
        if app_df['other_fields.n_comments'] != 0:
            try:
                driver.find_element(By.XPATH, '//*[@id="subtab_consulteeComments"]').click()
                summary_stats = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="commentsContainer"]/ul')))
                summary_stats = summary_stats.find_elements(By.XPATH, './li')[:2]
                summary_stat_strs = [stat.get_attribute('innerText').strip() for stat in summary_stats]
                summary_stat_nums = [int(re.search(r"\d+", stat_str).group()) for stat_str in summary_stat_strs]
                print(f"\n5.2. Consultee Comments: Consulted {summary_stat_nums[0]}, Responded {summary_stat_nums[1]}.")
                app_df['other_fields.n_comments_consultee_total_consulted'] = summary_stat_nums[0]
                app_df['other_fields.n_comments_consultee_responded'] = summary_stat_nums[1]
                app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_consultee_responded'] + \
                                                       app_df.at['other_fields.n_comments_public_received']

                if app_df['other_fields.n_comments_consultee_responded'] > 0:
                    scrape_comments(comment_source, comment_date, comment_content)
            except (NoSuchElementException, TimeoutException):
                print('\n5.2. Consultee Comments: sub-tab not found, skipped.')
                # Consultee Comments
                app_df['other_fields.n_comments_consultee_total_consulted'] = 0
                app_df['other_fields.n_comments_consultee_responded'] = 0
                # Total
                app_df.at['other_fields.n_comments'] = app_df.at['other_fields.n_comments_public_received']

        # ------------------------------------------------------------------
        # 6. Constraints (规划限制条件, 例如是否在保护区/洪泛区内等)
        # 6. Constraints (planning constraints, e.g. conservation area / flood zone, etc.)
        # ------------------------------------------------------------------
        def scrape_constraints():
            # 表格首行是表头, 需要跳过。/ The first row is the header row, skip it.
            constraint_table = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="caseConstraints"]/tbody')))
            rows = constraint_table.find_elements(By.XPATH, './tr')[1:]
            n_constraints = len(rows)
            app_df.at['other_fields.n_constraints'] = n_constraints
            print(f'\n6. Constraints: {n_constraints} items.') if PRINT else None

            if n_constraints > 0:
                constraint_columns = constraint_table.find_elements(By.XPATH, './tr[1]/th')
                constraint_names = [col.get_attribute('innerText').strip() for col in constraint_columns]
                constraints = {}
                for column_index, constraint_name in enumerate(constraint_names, start=1):
                    constraints[constraint_name] = [row.find_element(By.XPATH, f'./td[{column_index}]').get_attribute('innerText').strip() for row in rows]
                constraint_df = pd.DataFrame(constraints)
                constraint_df.to_csv(f"{folder_path}/constraints.csv", index=False)
                #self.upload_and_delete(folder_name=folder_name, file_name='constraints.csv') if CLOUD_MODE else None

        try:
            driver.find_element(By.XPATH, '//*[@id="tab_constraints"]').click()
            scrape_constraints()
        except (NoSuchElementException, TimeoutException):
            app_df.at['other_fields.n_constraints'] = 0
            print('\n6. Constraints: sub-tab not found, skipped.')

        # ------------------------------------------------------------------
        # 7. Documents (附件下载)
        # 7. Documents (attachment download), mode: documents
        # ------------------------------------------------------------------
        def get_n_documents(mode):
            if mode == 'documents':
                ### get n_documents ###
                documents_str = driver.find_element(By.XPATH, '//*[@id="tab_documents"]/span | //*[@id="pa"]/div[3]/div[3]/ul/li[3]/span').get_attribute('innerText').strip()
                #if documents_str is None:
                #    n_documents = 0
                match = re.search(r'\d+', documents_str) if documents_str else None
                if match:
                    n_documents = int(match.group())
                else:
                    n_documents = len(driver.find_elements(By.XPATH, '//*[@id="Documents"]/tbody/tr')) - 1 # tr[1:]
                return n_documents
            else:
                return 0

        try:
            current_url = driver.current_url
            driver.find_element(By.XPATH, '//*[@id="tab_documents"]').click()
            while driver.current_url == current_url:
                time.sleep(random.uniform(0.3, 0.7))

            # 获取文档界面的模式类型/get the mode of document pages.
            try:
                mode_str = driver.current_url.split('activeTab=')[1]
                mode = mode_str.split('&')[0]
            except IndexError as error:
                mode = 'associatedDocuments'

            # 分类型获取文档/get documents based on document mode.
            n_documents = get_n_documents(mode)
            print(f'\n7. Documents <{mode}>: {n_documents} items, folder_name: {folder_name}.') if PRINT else None

        except (NoSuchElementException, TimeoutException):
            n_documents = 0
            print('\n7. Documents: sub-tab not found, skipped.')

        app_df.at['other_fields.n_documents'] = n_documents
        if n_documents > 0:
            # document_names, file_urls = self.rename_documents_and_get_file_urls(response, self.data_upload_path, folder_name)
            file_urls, document_names = get_documents(driver, response, self.data_upload_path, folder_name, max_file_name_len)
            item = self.create_item(driver, folder_name, file_urls, document_names)
            yield item

        # ------------------------------------------------------------------
        # 8. Related Cases (用来反查 UPRN / 房产唯一编号)
        # 8. Related Cases (used to look up the UPRN / unique property reference number)
        # ------------------------------------------------------------------
        def get_related_properties_url():
            properties_panel = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="Property"]')))
            properties_str = properties_panel.find_element(By.XPATH, './h2/span | ./h3/span').get_attribute('innerText')
            match = re.search(r'\d+', properties_str) if properties_str else None
            n_properties = int(match.group()) if match else 0
            print(f'\n8. Related Cases: {n_properties} linked properties.') if PRINT else None

            property_url = None
            if n_properties == 1:
                property_url = properties_panel.find_element(By.XPATH, './ul/li/a').get_attribute('href')
            elif n_properties > 1:
                # 多个关联房产时, 用地址做模糊匹配, 找出最接近当前申请地址的那一个。
                # When several properties are linked, fuzzy-match against the application's
                # own address to find the closest one.
                properties = properties_panel.find_elements(By.XPATH, './ul/li')
                property_names = [p.find_element(By.XPATH, './a').get_attribute('innerText').strip() for p in properties]
                #properties = response.xpath('//*[@id="Property"]/ul/li')
                #property_names = [p.xpath('./a/text()').get().strip() for p in properties]
                try:
                    matched = difflib.get_close_matches(app_df.at['address'], property_names, n=1)[0]
                    matched_index = property_names.index(matched)
                    property_url = properties[matched_index].get_attribute('href')
                    #property_url = response.xpath(f'//*[@id="Property"]/ul/li[{matched_index + 1}]/a/@href').get()
                except IndexError:
                    pass
            return property_url

        try:
            driver.find_element(By.XPATH, '//*[@id="tab_relatedCases"]').click()
            property_url = get_related_properties_url()
            print('property url: ', property_url)
            if property_url:
                property_url = response.urljoin(property_url)
                yield SeleniumRequest(url=property_url, callback=self.parse_uprn_item, meta={'app_df': app_df})
            else:
                self.ending(app_df)

        except (NoSuchElementException, TimeoutException):
            print(f'\n8. Related Cases: 0 linked properties.') if PRINT else None
            self.ending(app_df)

    def parse_uprn_item(self, response):
        app_df = response.meta['app_df']
        uprn = response.xpath('//*[@id="propertyAddress"]/tbody/tr[1]/td/text()').get()
        if uprn:
            app_df.at['other_fields.uprn'] = uprn.strip()
            print(f"<UPRN> scraped: {app_df.at['other_fields.uprn']}") if PRINT else None
        self.ending(app_df)
