import os, re
from datetime import datetime
from pathlib import Path
import pandas as pd
from selenium.webdriver.common.by import By

# --- --- --- utils for I/O, paths, files. --- --- ---
def get_project_root() -> Path:
    return Path(__file__).parent.parent

def get_list_storage_path():
    return f'{Path(get_project_root()).parent}/Lists/'
    #return f'{Path(get_project_root()).parent}/Lists/'

def get_data_storage_path():
    return f'{Path(get_project_root()).parent}/ScrapedApplications/'
    #return f'{Path(get_project_root()).parent}/Lists_25/'

def get_IP_storage_path():
    return f'{Path(get_project_root()).parent}'

#def get_status_storage_path():
#    return f"{Path(get_project_root()).parent}/Scraper_Status/"

def get_filenames(src_path, ending=0):
    """
    Get all filenames from the source path.
    :param src_path: [String] The source path of files/folders
    :param ending: [Int] The number of files/folders. Will get all filenames if this param is not given.
    :return: [List] A list of filenames/dirnames in src_path.
    """
    try:
        filenames = os.listdir(src_path)
        filenames = [filename for filename in filenames if not filename.startswith('.')]
        filenames.sort(key=str.lower)
        if ending !=0:
            filenames = filenames[:ending]
        return filenames
    except:
        print(f'Failed to get files from the given path: {src_path}')
        return None


# --- --- --- utils for scraping basic data --- --- ---
# Used in CCED, Custom, Tascomi, Ocella, CivicaJason:
def scrape_data_items(app_df, items, item_values, details_dict, PRINT):
    for item, value in zip(items, item_values):
        item_name = item.get_attribute('innerText').strip()  #item.text.strip() # modified on 29-April-2026 for CivicaJason.
        data_name = details_dict[item_name]
        item_value = value.get_attribute('innerText').strip()  #value.text.strip()
        # print(i, item_name, item_value, type(item_name))
        try:
            app_df.at[data_name] = item_value
            print(f'    <{item_name}> scraped: {app_df.at[data_name]}') if PRINT else None
        # New
        except KeyError:
            app_df[data_name] = item_value
            print(f'    <{item_name}> scraped (new): {app_df.at[data_name]}') if PRINT else None
    return app_df

# Used in CCED, Tascomi (may not need unique_columns):
def scrape_for_csv(csv_name, table_columns, table_items, data_storage_path, folder_name, path='td'):
    content_dict = {}
    column_names = [column.text.strip() for column in table_columns]
    column_names = unique_columns(column_names)
    n_columns = len(column_names)

    for column_index in range(n_columns):
        content_dict[column_names[column_index]] = [table_item.find_element(By.XPATH, f'./{path}[{column_index+1}]').text.strip() for table_item in table_items]

    content_df = pd.DataFrame(content_dict)
    content_df.to_csv(f'{data_storage_path}{folder_name}/{csv_name}.csv', index=False)

#Used in CCED:
def scrape_multi_tables_for_csv(csv_names, tables, data_storage_path, folder_name, table_path='tbody/tr', column_path='th', item_path='td', PRINT=True):
    n_table_items = []
    for table_index, table in enumerate(tables):
        # table_name = table_names[table_index].text.strip().lower()
        table_rows = table.find_elements(By.XPATH, f'./{table_path}')
        table_columns = table_rows[0].find_elements(By.XPATH, f'./{column_path}')
        if len(table_columns) > 0:
            table_items = table_rows[1:]
            scrape_for_csv(csv_names[table_index], table_columns, table_items, data_storage_path, folder_name, path=item_path)
            print(f'{csv_names[table_index]}, {len(table_items)} items') if PRINT else None
            n_table_items.append(len(table_items))
        else:
            table_item = table_rows[0].find_element(By.XPATH, f'./{item_path}').text.strip()
            print(f"{csv_names[table_index]} <NULL>: {table_item}") if PRINT else None
            n_table_items.append(0)
    return n_table_items

def scrape_for_csv_single(self, csv_name, column_name, table_items, folder_name, path='td'):
    content_dict = {column_name: [table_item.find_element(By.XPATH, f'./{path}').text.strip() for table_item in table_items]}
    content_df = pd.DataFrame(content_dict)
    content_df.to_csv(f"{self.data_storage_path}{folder_name}/{csv_name}.csv", index=False)

# Used in scrape_for_csv(), and Custom:
def unique_columns(column_names):
    count_dict = {}
    unique_names = []
    for item in column_names:
        if item in count_dict:
            unique_names.append(f'{item}.{count_dict[item]}')
            count_dict[item] += 1
        else:
            unique_names.append(item)
            count_dict[item] = 1
    return unique_names

# --- --- --- other utils --- --- ---
def replace_invalid_characters(name):
    """ The following characters are forbidden in Windows/Linux directory names.
    < (less than)
    > (greater than)
    : (colon - sometimes works, but is actually NTFS Alternate Data Streams)
    " (double quote)
    / (forward slash)
    \ (backslash)
    | (vertical bar or pipe)
    ? (question mark)
    * (asterisk)
    #"""
    #for invalid_char in invalid_chars:
    #    if invalid_char in name:
    #        name = name.replace(invalid_char, '_')
    #return name
    return re.sub(r'[<>:"/\\|?*\n\r]+', '_', name)


def is_empty(value):
    """ updated on 02-08-2026
    判断 app_df 中某个字段是否"尚未填充"。
    Check whether a field in app_df is still unfilled / empty.
    """
    if pd.isnull(value):
        return True
    text = str(value).strip().lower()
    return text in ('', 'nan')

# --- --- --- 日期字符串匹配规则 --- --- ---
# Date string matching pattern
# 匹配形如 "Thu 02 Mar 2000" / "02 Mar 2000" / "02 March 2000" 的日期字符串。
# 星期几部分是可选的（不参与后续转换，只是允许它出现在字符串开头）。
# Matches date strings like "Thu 02 Mar 2000" / "02 Mar 2000" / "02 March 2000".
# The weekday part is optional and ignored during conversion — it's only
# allowed to appear at the start of the string.
DATE_PATTERN = re.compile(
    r'^\s*(?:[A-Za-z]{3,9}\s+)?'   # 可选星期几，如 Thu / Thursday / 周四(若有中文误判需求可另加) / optional weekday
    r'(\d{1,2})\s+'                # 日 (1或2位数字) / day (1-2 digits)
    r'([A-Za-z]{3,9})\s+'          # 月份英文缩写或全称 / month name, abbreviated or full
    r'(\d{4})\s*$'                 # 4位年份 / 4-digit year
)

def convert_date(text: str) -> str:
    """
    将英文日期字符串（如 "Thu 02 Mar 2000" 或 "02 March 2000"）转换为 "yyyy-mm-dd" 格式。
    若输入不符合日期格式，或日期本身不合法（如 30 Feb），则原样返回输入内容，并打印警告方便排查。
    Convert an English date string (e.g. "Thu 02 Mar 2000" or "02 March 2000") into "yyyy-mm-dd" format.
    If the input doesn't match the date pattern, or represents an invalid calendar date (e.g. 30 Feb), the original text is returned unchanged, with a warning printed for debugging.
    """
    if not isinstance(text, str):
        return text

    match = DATE_PATTERN.match(text.strip())
    if not match:
        return text  # 不是日期格式，原样返回 / not a date pattern, return unchanged

    day, month_str, year = match.groups()
    # 依次尝试 "月份缩写"(Jan/Feb/...) 和 "月份全称"(January/February/...) 两种格式
    # Try both abbreviated (Jan/Feb/...) and full (January/February/...) month names
    for fmt in ('%d %b %Y', '%d %B %Y'):
        try:
            dt = datetime.strptime(f'{day} {month_str} {year}', fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

    # 格式对了但日期不合法，或月份拼写有误（比如语言异常导致的损坏值）
    # Pattern matched but the date is invalid, or the month name is malformed (e.g. corrupted due to a locale issue).
    print(f'[convert_date] unparsable date string: "{text}"')
    return text

""" replaced by convert_date() on 02-08-2026 
def convert_date(date_string):
    strs = date_string.split(' ')
    if len(strs) > 2:
        year = strs[3]
        month = Month_Eng_to_Digit(strs[2])
        day = strs[1]
        return f'{day}-{month}-{year}'
    else:
        return date_string

def Month_Eng_to_Digit(month):
    # Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec
    # J x3, F, M x2, A x2, S, O, N, D
    first_str = month[0]
    if first_str == 'J':
        if month[1] == 'a':
            return '01'  # Jan
        elif month[2] == 'n':
            return '06'  # Jun
        else:
            return '07'  # Jul
    elif first_str == 'M':
        if month[2] == 'r':
            return '03'  # Mar
        else:
            return '05'  # May
    elif first_str == 'A':
        if month[1] == 'p':
            return '04'  # Apr
        else:
            return '08'  # Aug
    elif first_str == 'F':
        return '02'  # Feb
    elif first_str == 'S':
        return '09'  # Sep
    elif first_str == 'O':
        return '10'  # Oct
    elif first_str == 'N':
        return '11'  # Nov
    else:
        return '12'  # Dec
"""
