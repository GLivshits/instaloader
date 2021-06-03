import requests
import numpy as np
import time
from functools import partial

class ProxyRotator(object):

    def __init__(self, api_key, idx):
        self.api_key = api_key
        self.idx = idx
        self.current_params = self.get_current_proxy_params()
        self.rotate_ip()

    def get_current_proxy_params(self):
        # print('FUNCTION EXECUTED:{}'.format(inspect.currentframe().f_code.co_name))
        sess = requests.Session()
        req_dict = {}
        cur_attempt = 1
        while len(req_dict) < 1:
            print('Trying to retrieve current proxy. Current attempt: {}.'.format(cur_attempt))
            resp = sess.get(url='https://mobileproxy.space/api.html?command=get_my_proxy',
                            headers={'Authorization': 'Bearer {}'.format(self.api_key)}, timeout = 5)
            proxy = resp.json()[self.idx]
            if isinstance(proxy, dict):
                if 'proxy_login' in proxy.keys():
                    break
            else:
                time.sleep(1)
                cur_attempt += 1
        print('Success!')
        sess.close()
        return proxy

    def get_proxy_url_format(self, port_type = 'socks5'):
        assert port_type in ['http', 'socks5']
        port_type = 'http'
        # print('FUNCTION EXECUTED:{}'.format(inspect.currentframe().f_code.co_name))
        https_proxy = 'http://{}:{}@{}:{}'.format(self.current_params['proxy_login'],
                                                 self.current_params['proxy_pass'],
                                                 self.current_params['proxy_hostname'],
                                                 self.current_params['proxy_http_port'])
        http_proxy = 'http://{}:{}@{}:{}'.format(self.current_params['proxy_login'],
                                                 self.current_params['proxy_pass'],
                                                 self.current_params['proxy_hostname'],
                                                 self.current_params['proxy_socks5_port'])
        return {'https': https_proxy, 'http': http_proxy}

    def rotate_ip(self):
        sess = requests.Session()
        cur_attempt = 1
        sess.request = partial(sess.request, timeout=30)
        while True:
            print('Trying to rotate IP. Attempt {}.'.format(cur_attempt))
            resp = sess.get(url = 'https://mobileproxy.space/reload.html?login={}&pass={}&port={}'.format(self.current_params['proxy_login'],
                                                                                                          self.current_params['proxy_pass'],
                                                                                                          self.current_params['proxy_http_port']))
            resp = resp.json()
            if isinstance(resp, dict):
                if 'new_ip' in resp.keys():
                    print('New IP is {}'.format(resp['new_ip']))
                    break
                else:
                    cur_attempt += 1
                    time.sleep(1)
            elif cur_attempt < 5:
                time.sleep(1)
                cur_attempt += 1
            else:
                print('IP change failed. Something is wrong with proxy.')
                raise ConnectionError
        sess.close()
        self.current_params = self.get_current_proxy_params()

    def change_proxy_workstation(self):
        sess = requests.Session()
        cur_attempt = 1
        while True:
            resp = sess.get(url = 'https://mobileproxy.space/reload.html?login={}&pass={}&port={}'.format(self.current_params['proxy_login'],
                                                                                                          self.current_params['proxy_pass'],
                                                                                                          self.current_params['proxy_http_port']))
            resp = resp.json()
            if isinstance(resp, dict):
                if 'new_ip' in resp.keys():
                    print('New IP is {}'.format(resp['new_ip']))
                    break
            else:
                time.sleep(1)
                cur_attempt += 1
        sess.close()
        self.current_params = self.get_current_proxy_params()




