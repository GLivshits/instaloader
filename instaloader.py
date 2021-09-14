#!/usr/bin/env python3

import argparse
import concurrent.futures
import time
from functools import partial, partialmethod

from instaloader import scrape_followers, scrape_user_data, scrape_posts, scrape_hashtag, scrape_location
from instaloader.proxyrotator import ProxyRotator
from instaloader.exceptions import InstaloaderException, QueryReturnedNotFoundException, ProfileNotExistsException
from instaloader.instaloader import Instaloader
from instaloader.instaloadercontext import default_user_agent
import pandas as pd
from tqdm import tqdm
from multiprocessing import Process
import os
from instaloader.utils import get_profile_struct

def delete_row(df, row_idx):
    df = df.drop(row_idx)
    return df



parser = argparse.ArgumentParser()
parser.add_argument('profiles', nargs='*',
                       help="Download profile. If an already-downloaded profile has been renamed, Instaloader "
                            "automatically finds it by its unique ID and renames the folder likewise.")
parser.add_argument('--csv_path', type = str,
                    help = 'Path to the .csv file from which scraping will be conducted')
parser.add_argument('--task', type = str, choices = ['scrape_user_data', 'scrape_posts', 'scrape_followers', 'scrape_hashtag', 'scrape_location'],
                    help = 'Task specifies what action will be performed on data.', required = True)
parser.add_argument('--use_proxy', action = 'store_true',
                    help = 'Whether to use proxy or not. For followers scraping its not used.')
parser.add_argument('--api_key', type = str, default = '',
                    help = 'Whether to use proxy or not. For followers scraping its not used.')
parser.add_argument('--proxy_index', type = int, default = 0,
                    help = 'Index of proxy (if used).')
parser.add_argument('--download_profile_pic', action = 'store_true', help = 'Bool flag to download profile title pic')
parser.add_argument('--download_pictures', action = 'store_true',
                    help = 'Bool flag to download pictures from posts. It is highly recommended not to use this function'
                           'due to slow download rate and high chance to get banned. Download metadata (jsons) instead.')
parser.add_argument('--download_videos', action = 'store_true',
                    help = 'Bool flag to download videos from posts. On the state of affairs on 3 Sept, 2021 metadata'
                           'contains only link to thumbnail, but not video.')
parser.add_argument('--download_video_thumbnails', action = 'store_true',
                    help = 'Bool flag to download video thumbnails from posts. It is highly recommended '
                           'not to use this function due to slow download rate and high chance to get banned. '
                           'Download metadata (jsons) instead.')
parser.add_argument('--download_metadata', action = 'store_true',
                    help = 'Bool flag to download posts metadata (jsons). They are saved in compressed LZMA format. '
                           'A download_images.py script is used to download all pics from scraped metadata! Use it in '
                           'combination with this flag for scraping huge number of profile media.')
parser.add_argument('--username', type = str, help = 'Instagram username for login')
parser.add_argument('--password', type = str, help = 'Instagram password for login')

args = parser.parse_args()

if len(args.profiles) > 0:
    assert args.csv_path is None, 'Both profiles and csv_path cannot be specified!'
else:
    assert isinstance(args.csv_path, str), 'If profiles not specified, you should specify the path to .csv file!'

if args.use_proxy:
    if len(args.api_key) == 0:
        raise InstaloaderException('To use proxy, one should specify an api-key from mobileproxy.space! Proceeding without proxy.')


api_key = args.api_key
proxy_objects = []
if args.use_proxy:
    for i in range(args.proxy_index + 1):
        proxy_objects.append(ProxyRotator(api_key = api_key, idx = i))
else:
    proxy_objects.append(None)

loaders = []
for proxy in proxy_objects:
    loader = Instaloader(sleep=True, quiet=False, user_agent='{}'.format(default_user_agent()),
                            dirname_pattern = os.path.join('data', '{target}'), filename_pattern='{target}_{date_utc}',
                            download_pictures = args.download_pictures,
                            download_profile_pic = args.download_profile_pic,
                            download_videos = args.download_videos,
                            download_video_thumbnails = args.download_video_thumbnails,
                            download_geotags = False,
                            download_comments = False,
                            save_metadata = args.download_metadata,
                            compress_json = True,
                            post_metadata_txt_pattern = '',
                            storyitem_metadata_txt_pattern = None,
                            max_connection_attempts = 2,
                            request_timeout = 15.0,
                            resume_prefix = 'iterator',
                            check_resume_bbd = False,
                            proxyrotator = proxy)
    loaders.append(loader)

func_dict = {'scrape_user_data': scrape_user_data, 'scrape_posts': scrape_posts,
             'scrape_followers': scrape_followers, 'scrape_hashtag': scrape_hashtag,
             'scrape_location': scrape_location}
func = func_dict[args.task]._main
# func = partial(func, username = args.username, password = args.password)
if args.csv_path:
    df = pd.read_csv(args.csv_path, engine='python', sep=';')
    if 'downloaded' not in df.columns: # append download status identifier to skip already downloaded profiles after break
        df['downloaded'] = False
    df['id'] = df['id'].astype(str)
    df['id'] = df['id'].fillna('nan')
    df['username'] = df['username'].astype(str)
    df['username'] = df['username'].fillna('nan')
    ids = df[['id', 'username']][~df['downloaded']]
    ids['id'] = ids['id'].astype(str)
    ids['username'] = ids['username'].astype(str)
    ids['idx'] = ids.index
    ids = ids.to_dict('records')
else:
    ids = []
    df = None
    for item in args.profiles:
        if item.isdigit():
            ids.append({'id': int(item), 'username': 'nan'})
        else:
            ids.append({'id': 'nan', 'username': item})

ids_container = iter(ids)

flag = True
total_index = 0
def subprocess(loader, target):
    global total_index, df
    total_index += 1
    print('Current total index is {}.'.format(total_index))
    try:
        if args.task not in ['scrape_hashtag', 'scrape_location']:
            target = get_profile_struct(loader, target)

        func(loader, target)
        total_index += 1
        if not (df is None):
            df.loc[target['idx'], 'downloaded'] = True
            if (total_index % 100 == 0):
                df.to_csv(args.csv_path, sep = ';', index = None)
    except (QueryReturnedNotFoundException, ProfileNotExistsException):
        if not (df is None):
            df = delete_row(df, target['idx'])
        pass
    except KeyboardInterrupt:
        if not (df is None):
            df.to_csv(args.csv_path, sep = ';', index = None)
        raise
    except Exception as err:
        print(err)
        raise err




num_processes = 1 if (not args.use_proxy) else (args.proxy_index + 1)
if num_processes == 1:
    loader = loaders[0]
    del loaders
    for item in ids_container:
        subprocess(loader, item)
else:
    processes = []
    for item in loaders:
        processes.append(Process(subprocess, args = (item, next(ids_container))))

print('Ready!')


# executor = concurrent.futures.ProcessPoolExecutor(max_workers=args.proxy_index + 1)
#
#
# jobs = [executor.submit(subprocess, target, index) for index, target  in enumerate(ids)]
# for _ in tqdm(concurrent.futures.as_completed(jobs), total = len(jobs)):
#     pass

# if __name__ == '__main__':
#     k = 1
#     while flag:
#         print('Current attempt: {}.'.format(k))
#         try:
#             func.main(profiles = args.profiles ,filename = args.csv_path, proxy_object=proxy_object, compress_json = True)
#             flag = False
#         except KeyboardInterrupt:
#             flag = False
#             break
#         except Exception as err:
#             print('Following error occured:\n{}\nDoing sleep for 60 sec, then retry.'.format(str(err)))
#             time.sleep(60)
#             k += 1
#             flag = False
#             break

