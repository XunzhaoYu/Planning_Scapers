import os
import pandas as pd
from tools.utils import get_storage_path, get_temp_storage_path, get_csv_files


def append_all(temp=True):
    """
    Append all csv files to a single csv file.
    :param temp: [Bool] Are csv files stored in the temp storage folder?
    """
    if temp:
        csv_files = get_csv_files(get_temp_storage_path())
        append_df = pd.concat([pd.read_csv(file)[::-1] for file in csv_files], ignore_index=True)
    else:
        csv_files = get_csv_files(get_storage_path())
        append_df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)
    append_df.to_csv('../UKPlanning.csv', index=False)


def inverse_index(start_dir=1, end_dir=424):
    """
    The raw data scraped from the PlanIt API is stored in an inverse order, this method will make data stored in a chronological order.
    :param start_dir: [Int] The index of the first folder
    :param end_dir: [Int] The index of the last folder
    """
    csv_files = get_csv_files(get_temp_storage_path(), start_dir=start_dir-1, end_dir=end_dir)
    for file_path in csv_files:
        #print(file_path)
        df = pd.read_csv(file_path, index_col=0)
        df = df[::-1]
        df.to_csv(file_path)


def append_by_year(start_dir=1, end_dir=424):
    """
    Append csv files from each authority by years.
    :param start_dir: [Int] The index of the first folder
    :param end_dir: [Int] The index of the last folder
    """
    for dir in range(start_dir-1, end_dir):
        csv_files = get_csv_files(get_temp_storage_path(), start_dir=dir, end_dir=dir+1)

        first_filename = csv_files[0]
        prefix = first_filename.split('=')[0] + '='
        #print(f"prefix: {prefix}")
        first_year = first_filename.split('=')[1][:4]
        last_year = csv_files[-1].split('=')[1][:4]
        print(f"first year: {first_year}, last year: {last_year}")
        auth = first_filename.split('/')[-2]

        storage_path = f"{get_storage_path()}{auth}/"
        print(f"storage path: {storage_path}")
        if not os.path.exists(storage_path):
            os.mkdir(storage_path)

        end_index = 0
        for year in range(int(first_year), int(last_year)):
            start_index = end_index
            while csv_files[end_index].startswith(prefix+str(year)):
                end_index += 1
            print(f"{year}: {start_index} to {end_index}")
            year_df = pd.concat([(pd.read_csv(file)) for file in csv_files[start_index:end_index]], ignore_index=True)
            year_df.to_csv(f"{storage_path}{auth}{year}.csv", index=False)
        year_df = pd.concat([(pd.read_csv(file)) for file in csv_files[end_index:]], ignore_index=True)
        year_df.to_csv(f"{storage_path}{auth}{last_year}.csv", index=False)


