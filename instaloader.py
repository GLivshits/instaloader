#!/usr/bin/env python3

import argparse
import concurrent.futures
import time

from instaloader import scrape_followers, scrape_user_data, scrape_posts, scrape_hashtag, scrape_location
from instaloader.proxyrotator import ProxyRotator
from instaloader.exceptions import InstaloaderException, QueryReturnedNotFoundException, ProfileNotExistsException
from instaloader.instaloader import Instaloader
from instaloader.instaloadercontext import default_user_agent
import pandas as pd
from tqdm import tqdm

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
    if len(api_key) > 0:
        for i in range(args.proxy_index + 1):
            proxy_objects.append(ProxyRotator(api_key = api_key, idx = i))
    else:
        proxy_objects = [None]
else:
    args.proxy_index = 0

loaders = []
for proxy in proxy_objects:
    loader = Instaloader(sleep=True, quiet=False, user_agent='{}'.format(default_user_agent()),
                            dirname_pattern='data/{target}', filename_pattern='{target}_{date_utc}',
                            download_pictures = False,
                            download_videos = False,
                            download_video_thumbnails = False,
                            download_geotags = False,
                            download_comments=False,
                            save_metadata = True,
                            compress_json = True,
                            post_metadata_txt_pattern='',
                            storyitem_metadata_txt_pattern=None,
                            max_connection_attempts=2,
                            request_timeout=15.0,
                            resume_prefix='iterator',
                            check_resume_bbd=False,
                            rapidapi_key=None, proxyrotator = proxy)
    loaders.append(loader)

func_dict = {'scrape_user_data': scrape_user_data, 'scrape_posts': scrape_posts,
             'scrape_followers': scrape_followers, 'scrape_hashtag': scrape_hashtag,
             'scrape_location': scrape_location}
func = func_dict[args.task]

df = pd.read_csv(args.csv_path, engine='python', sep=';')
if 'downloaded' not in df.columns:
    df['downloaded'] = False
df['id'] = df['id'].astype(str)
df['username'] = df['username'].astype(str)
ids = df[['id', 'username']][~df['downloaded']]
ids['id'] = ids['id'].astype(str)
ids['username'] = ids['username'].astype(str)
ids['idx'] = ids.index
ids = ids.to_dict('records')


flag = True
executor = concurrent.futures.ProcessPoolExecutor(max_workers=args.proxy_index + 1)
def subprocess(target, total_index):
    global df
    total_index += 1
    print('Current total index is {}.'.format(total_index))
    try:
        func._main(instaloader = loaders[total_index % (args.proxy_index + 1)], target = target)
        df.loc[target['idx'], 'downloaded'] = True
        total_index += 1
        if (total_index % 100 == 0):
            df.to_csv(args.csv_path, sep = ';', index = None)
    except (QueryReturnedNotFoundException, ProfileNotExistsException):
        df = delete_row(df, target['idx'])
        pass
    except KeyboardInterrupt:
        df.to_csv(args.csv_path, sep=';', index=None)
        raise
    except Exception as err:
        print(err)

jobs = [executor.submit(subprocess, target, index) for index, target  in enumerate(ids)]
for _ in tqdm(concurrent.futures.as_completed(jobs), total = len(jobs)):
    pass

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

