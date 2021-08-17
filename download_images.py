import requests
import lzma
import json
from typing import Union
import os
import shutil
import concurrent.futures
import argparse
from tqdm import tqdm

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

def scrape_pics(path, save_path, num_threads):
    profile_name = path.split(os.path.sep)[-1]
    image_path = os.path.join(save_path, profile_name)
    posts_path = os.path.join(path, 'posts')
    if not os.path.exists(posts_path):
        return
    jsons = list(map(lambda x: os.path.join(posts_path, x), os.listdir(posts_path)))
    if os.path.exists(image_path):
        if len(os.listdir(image_path)) >= len(jsons):
            return
    if len(jsons) == 0:
        return
    os.makedirs(image_path, exist_ok = True)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers = num_threads)
    def subprocess(path_to_json, i):
        try:
            with lzma.open(path_to_json, 'r') as f:
                item = json.load(f)
            resp = requests.get(item['node']['display_url'], timeout = 100)
            if resp.status_code == 200:
                filename = '{}.jpg'.format(i)
                write_raw(resp.content, os.path.join(image_path, filename))
            elif (resp.status_code == 403) and (resp.text == 'URL signature expired'):
                raise JsonExpired
            children = item.get('edge_sidecar_to_children', {}).get('edges', [])
            k = 0
            for child in children:
                resp = requests.get(child['display_url'])
                if resp.status_code == 200:
                    filename = '{}_child{}.jpg'.format(i, k)
                    write_raw(resp.content, os.path.join(image_path, filename))
                    k += 1
        except KeyboardInterrupt:
            raise
        except JsonExpired:
            os.remove(path_to_json)
        except Exception as err:
            pass
    a = 0
    jobs = [executor.submit(subprocess, item, i) for i, item in enumerate(jsons)]
    for _ in concurrent.futures.as_completed(jobs):
        a += 1



def main():
    parser = argparse.ArgumentParser(description = 'Args for merging jsons.')
    parser.add_argument('--path', type = str, required = True, help = 'Path to folder with all profiles (each profile as unique folder).')
    parser.add_argument('--save-path', type = str, required = True, help = 'Path to save images')
    parser.add_argument('--num-workers', type = int, default = None, help = 'How many cores to utilize.')
    parser.add_argument('--threads-per-worker', type = int, default = 1)
    args = parser.parse_args()
    os.makedirs(args.save_path, exist_ok = True)
    all_paths = list(map(lambda x: os.path.join(args.path, x), os.listdir(args.path)))
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=args.num_workers)
    jobs = [executor.submit(scrape_pics, item, args.save_path, args.threads_per_worker) for item in all_paths]
    for _ in tqdm(concurrent.futures.as_completed(jobs), total=len(jobs)):
       pass

main()
