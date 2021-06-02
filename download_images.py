import requests
import lzma
import json
from typing import Union
import os
import shutil
import concurrent.futures
import argparse


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

def scrape_pics(path):
    sess = requests.Session()
    image_path = os.path.join(path, 'images')
    os.makedirs(image_path, exist_ok = True)
    i = 1
    with lzma.open(os.path.join(path, 'all_posts.json.xz')) as f:
        content = json.load(f)
    for item in content:
        try:
            resp = sess.get(item['display_url'])
            if resp.status_code == 200:
                write_raw(resp.content, os.path.join(image_path, '{}.jpg'.format(i)))
            i += 1
        except:
            continue

def main():
    parser = argparse.ArgumentParser(description = 'Args for merging jsons.')
    parser.add_argument('--path', type = str, required = True, help = 'Path to folder with all profiles (each profile as unique folder).')
    parser.add_argument('--num-workers', type = int, default = None, help = 'How many cores to utilize.')
    args = parser.parse_args()
    all_paths = list(map(lambda x: os.path.join(args.path, x), os.listdir(args.path)))
    with concurrent.futures.ProcessPoolExecutor(max_workers = args.num_workers) as executor:
        executor.map(scrape_pics, all_paths)

if __name__ == '__main__':
    main()
