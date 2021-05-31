#!/usr/bin/env python3

from instaloader.my_main import main
from instaloader.proxyrotator import ProxyRotator
import time

filename = input('Please provide filepath to .csv file.\n')
proxy_index = input('Which proxy to use?\n')
proxy_index = int(proxy_index)
api_key = '9b730336c403334c7a8ae628aa93ad9f'
proxy_object = ProxyRotator(api_key = api_key, idx = proxy_index)


if __name__ == '__main__':
    k = 1
    while True:
        print('Current attempt: {}.'.format(k))
        try:
            main(filename, proxy_object)
        except KeyboardInterrupt:
            break
        except:
            time.sleep(60)
            k += 1
# if __name__ == '__main__':
#     main()

