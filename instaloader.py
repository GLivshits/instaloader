#!/usr/bin/env python3

from instaloader import scrape_followers, scrape_user_data, scrape_posts
from instaloader.proxyrotator import ProxyRotator
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--csv_path', required = True, type = str,
                    help = 'Path to the .csv file from which scraping will be conducted')
parser.add_argument('--task', type = 'str', choices = ['scrape_user_data', 'scrape_posts', 'scrape_followers'],
                    help = 'Task specifies what action will be performed on data.', required = True)
parser.add_argument('--use_proxy', action = 'store_true',
                    help = 'Whether to use proxy or not. For followers scraping its not used.')
parser.add_argument('--proxy_index', type = int, default = 0,
                    help = 'Index of proxy (if used).')
args = parser.parse_args()

api_key = ''
proxy_object = None
if len(api_key) > 0 and args.use_proxy:
    proxy_object = ProxyRotator(api_key = api_key, idx = args.proxy_index)

func_dict = {'scrape_user_data': scrape_user_data, 'scrape_posts': scrape_posts, 'scrape_followers': scrape_followers}
func = func_dict[args.task]

if __name__ == '__main__':
    k = 1
    while True:
        print('Current attempt: {}.'.format(k))
        try:
            func(args.csv_path)
        except KeyboardInterrupt:
            break
        except Exception as err:
            print('Following error occured:\n{}\nDoing sleep for 60 sec, then retry.'.format(str(err)))
            time.sleep(60)
            k += 1
# if __name__ == '__main__':
#     main()

