import requests
import lzma
import json
from typing import Union
import os
import shutil
import concurrent.futures
import argparse
from tqdm import tqdm
import pandas as pd
import numpy as np

class JsonExpired(Exception):
    """Base exception for this script.

    :note: This exception should not be raised directly."""
    pass


def write_raw(resp: Union[bytes, requests.Response], filename: str) -> None:
    # print('FUNCTION EXECUTED:{}'.format(inspect.currentframe().f_code.co_name))
    """Write raw response data into a file.

    .. versionadded:: 4.2.1"""
    with open(filename + '.temp', 'wb') as file:
        if isinstance(resp, requests.Response):
            shutil.copyfileobj(resp.content, file)
        else:
            file.write(resp)
    os.replace(filename + '.temp', filename)

def scrape_pics(row, save_path):
    resp = requests.get(row['display_url'], timeout = 100)
    if resp.status_code == 200:
        write_raw(resp.content, os.path.join(save_path, '{}_{}.jpg'.format(row['owner.id'], row['id'])))

def main():
    parser = argparse.ArgumentParser(description = 'Args for merging jsons.')
    parser.add_argument('--csv_path', type = str, required = True, help = 'Path to folder with all profiles (each profile as unique folder).')
    parser.add_argument('--save_path', type = str, required = True, help = 'Path to save images')
    parser.add_argument('--num-workers', type = int, default = None, help = 'How many cores to utilize.')
    args = parser.parse_args()
    os.makedirs(args.save_path, exist_ok = True)
    df = pd.read_csv(args.csv_path, sep = ',')
    bad_words = ['sale', 'discount', 'offer', 'price', 'ship', 'special', '%', 'shop', 'pcs', 'usd', 'cm', 'kg', 'material', 'stock', '*']

    def remove_bad_posts(df, bad_words):
        idxs = []
        for item in bad_words:
            for idx, row in df.iterrows():
                if item in row['edge_media_to_caption.edges'].lower():
                    idxs.append(idx)
        idxs = np.unique(idxs)
        df = df.drop(index=idxs)
        return df

    print('Removing bad words')

    df = remove_bad_posts(df, bad_words)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.num_workers)
    jobs = [executor.submit(scrape_pics, row, args.save_path) for _, row in df.iterrows()]
    for _ in tqdm(concurrent.futures.as_completed(jobs), total=len(jobs)):
       pass
    #for _, row in tqdm(df.iterrows()):
     #   scrape_pics(row, args.save_path)

main()
