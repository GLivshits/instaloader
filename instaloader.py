#!/usr/bin/env python3

from instaloader import scrape_followers, scrape_user_data, scrape_posts, scrape_hashtag
from instaloader.proxyrotator import ProxyRotator
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('profiles', nargs='*',
                       help="Download profile. If an already-downloaded profile has been renamed, Instaloader "
                            "automatically finds it by its unique ID and renames the folder likewise.")
parser.add_argument('--csv_path', type = str,
                    help = 'Path to the .csv file from which scraping will be conducted')
parser.add_argument('--task', type = str, choices = ['scrape_user_data', 'scrape_posts', 'scrape_followers', 'scrape_hashtag'],
                    help = 'Task specifies what action will be performed on data.', required = True)
parser.add_argument('--use_proxy', action = 'store_true',
                    help = 'Whether to use proxy or not. For followers scraping its not used.')
parser.add_argument('--proxy_index', type = int, default = 0,
                    help = 'Index of proxy (if used).')
args = parser.parse_args()

if len(args.profiles) > 0:
    assert args.csv_path is None, 'Both profiles and csv_path cannot be specified!'
else:
    assert isinstance(args.csv_path, str), 'If profiles not specified, you should specify the path to .csv file!'

api_key = ''
proxy_object = None
if len(api_key) > 0 and args.use_proxy:
    proxy_object = ProxyRotator(api_key = api_key, idx = args.proxy_index)

func_dict = {'scrape_user_data': scrape_user_data, 'scrape_posts': scrape_posts,
             'scrape_followers': scrape_followers, 'scrape_hashtag': scrape_hashtag}
func = func_dict[args.task]

flag = True
if __name__ == '__main__':
    k = 1
    while flag:
        print('Current attempt: {}.'.format(k))
        try:
            func.main(profiles = args.profiles ,filename = args.csv_path, proxy_object=proxy_object, compress_json = True)
            flag = False
        except KeyboardInterrupt:
            flag = False
            break
        except Exception as err:
            print('Following error occured:\n{}\nDoing sleep for 60 sec, then retry.'.format(str(err)))
            time.sleep(60)
            k += 1

