import os, re
from pathlib import Path
import pandas as pd

# --- --- --- utils for I/O, paths, files. --- --- ---
def get_project_root() -> Path:
    return Path(__file__).parent.parent

def get_list_storage_path():
    return f"{Path(get_project_root()).parent}/Lists/"
    #return f"{Path(get_project_root()).parent}/Lists_Summary/"

def get_data_storage_path():
    return f"{Path(get_project_root()).parent}/ScrapedApplications/"
    #return f"{Path(get_project_root()).parent}/Lists_Temp23/"

def get_IP_storage_path():
    return f"{Path(get_project_root()).parent}"

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
        print("Failed to get files from the given path:", src_path)
        return None


# --- --- --- utils for scraping basic data --- --- ---
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
        return f"{day}-{month}-{year}"
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