import os, re
from pathlib import Path
import pandas as pd
from selenium.webdriver.common.by import By

# --- --- --- utils for I/O, paths, files. --- --- ---
def get_project_root() -> Path:
    return Path(__file__).parent.parent

def get_list_storage_path():
    return f'{Path(get_project_root()).parent}/Lists/'
    #return f'{Path(get_project_root()).parent}/Lists_Summary/'

def get_data_storage_path():
    return f'{Path(get_project_root()).parent}/ScrapedApplications/'
    #return f'{Path(get_project_root()).parent}/Lists_Temp23/'

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
# Used in CCED, Tascomi:
def scrape_data_items(app_df, items, item_values, details_dict, PRINT):
    for item, value in zip(items, item_values):
        item_name = item.text.strip()
        data_name = details_dict[item_name]
        item_value = value.text.strip()
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

# Used in scrape_for_csv().
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

def is_empty(cell):
    return pd.isnull(cell)

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