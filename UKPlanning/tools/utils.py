from pathlib import Path
import os
import numpy as np


def get_project_root() -> Path:
    return Path(__file__).parent.parent

def get_storage_path():
    return f"{Path(get_project_root()).parent}/Data/"

def get_temp_storage_path():
    return f"{Path(get_project_root()).parent}/Data_Temp/"

def get_pages(authority):
    """
    Due to the limitation of the PlanIt API, the data scraped from some authorities cannot be stored in a single page (Error Code: 400).
    Therefore, it is necessary to set different page numbers for these authorities.
    This method gets the number of pages required to store the data scraped from a given authority.
    :param authority: [String] The name of an authority.
    :return: [Int] Number of pages.
    #[18, 28, 87, 93, 107, 129, 141, 399, 407, 408]
    """
    if authority in ['Barnet', 'Croydon', 'Edinburgh', 'FermanaghOmagh', 'Kingston', 'Lewisham', 'MoleValley', 'Wigan']:
        return 2
    elif authority in ['Birmingham', 'DorsetCouncil', 'Leeds', 'Westminster', 'Wiltshire']:
        return 3
    elif authority == 'Cornwall':
        return 4
    else:
        return 1

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


def get_csv_files(src_path, start_dir = 0, end_dir = 424):
    """
    Get csv files' paths.
    :param src_path: [String] The storage path of csv files
    :param start_dir: [Int] The index of the first folder
    :param end_dir: [Int] The index of the last folder
    :return: [List] A list of csv files' paths.
    """
    csv_files = []
    # Get the number of scraped authorities / the number of folders.
    dirnames = get_filenames(src_path)
    n_dirs = len(dirnames)

    # For robustness.
    start_dir = np.minimum(np.maximum(start_dir, 0), n_dirs)
    end_dir = np.minimum(np.maximum(end_dir, 0), n_dirs)
    assert(start_dir <= end_dir)

    for dir_index in range(start_dir, end_dir):
        dirname = dirnames[dir_index]
        print(f"{dir_index+1}/{n_dirs}: {dirname}")
        dir_path = f"{src_path}{dirname}/"
        filenames = get_filenames(dir_path)
        for filename in filenames:
            csv_files.append(dir_path + filename)
    return csv_files

