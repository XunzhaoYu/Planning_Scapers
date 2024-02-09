import pandas as pd
from tools.utils import get_storage_path, get_csv_files


def validate_amount(start_dir=1, end_dir=424):
    scrapers_df = pd.read_csv('scraper_name.csv')
    n_apps = scrapers_df.iloc[:, 3]
    n_apps = n_apps[:-1]

    suspects = []
    for dir in range(start_dir-1, end_dir):
        csv_files = get_csv_files(get_storage_path(), start_dir=dir, end_dir=dir+1)
        auth_df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)
        n_apps_in_auth = auth_df.shape[0]
        percent = n_apps_in_auth * 100.0 / n_apps[dir]
        reasonable = 97<=percent<=100
        print("Scraped {:d}/ Total {:d} = {:.4f}%, {} \n".format(n_apps_in_auth, n_apps[dir], percent, reasonable))
        if not reasonable:
            suspects.append(dir)
    return suspects


def validate_authority(start_dir=1, end_dir=424):
    scrapers_df = pd.read_csv('scraper_name.csv')
    scrapers = scrapers_df.iloc[:, 0]
    scrapers = scrapers[:-1]

    for dir in range(start_dir-1, end_dir):
        csv_files = get_csv_files(get_storage_path(), start_dir=dir, end_dir=dir+1)
        auth_df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)
        scrapers_in_auth = auth_df.iloc[:, 0]

        correct = 0
        mistake = 0
        for app in range(auth_df.shape[0]):
            if scrapers_in_auth[app].startswith(scrapers[dir]):
                correct += 1
            else:
                mistake += 1
                print(scrapers_in_auth[app])
        print(f"correct: {correct}, mistake: {mistake}. \n")