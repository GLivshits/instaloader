import os
import re
import sys
import json
from typing import Optional, Dict

from . import Instaloader, InstaloaderException, Profile, ProfileNotExistsException, \
    TwoFactorAuthRequiredException, BadCredentialsException, QueryReturnedNotFoundException
from .instaloadercontext import default_user_agent
from .exceptions import LoginRequiredException
import pandas as pd

def modify_json(filename, data):
    with open(filename, 'r') as f:
        json_results = json.load(f)
    for item in data:
        json_results.append(item)
    with open(filename, 'w') as f:
        json.dump(json_results, f)

def _main(instaloader: Instaloader,
          profile: Profile,
          username: str,
          password: str,
          sessionfile: Optional[str] = None,
          max_count: Optional[int] = 20000,
          scrape_followers: bool = False, scrape_followees: bool = True) -> None:

    instaloader.two_factor_login(username, password)
    followers_flag = True
    followees_flag = True
    while followees_flag or followees_flag:
        try:
            while followers_flag:
                if instaloader.context.is_logged_in and scrape_followers:
                    data = []
                    kk = 0
                    for followee in profile.get_followers():
                        data.append({'id': str(followee._asdict()['id']), 'username': followee._asdict()['username'],
                                     'biography': '', 'business_category_name': '', 'connected_fb_page': '',
                                     'external_url': '', 'full_name': followee._asdict()['full_name'],
                                     'is_business_account': '', 'is_joined_recently': '',
                                     'is_private': '', 'is_professional_account': '', 'is_verified': '',
                                     'profile_pic_url': followee._asdict()['profile_pic_url']})
                        kk += 1
                        if kk % 100 == 0:
                            print('Downloaded {} users.'.format(kk))
                        if kk == max_count:
                            print('Breaking loop since max_count is reached.')
                            break
                    df = pd.json_normalize(data)
                    df.to_csv(instaloader.format_filename_within_target_path(profile.userid.lower(),
                                                                             profile,
                                                                             'profile',
                                                                             'followers',
                                                                             'csv'))
                    followers_flag = False
                    del data

            while followees_flag:
                if instaloader.context.is_logged_in and scrape_followees:
                    data = []
                    kk = 0
                    for followee in profile.get_followees():
                        data.append({'id': str(followee._asdict()['id']), 'username': followee._asdict()['username'],
                                     'biography': '', 'business_category_name': '', 'connected_fb_page': '',
                                     'external_url': '', 'full_name': followee._asdict()['full_name'],
                                     'is_business_account': '', 'is_joined_recently': '',
                                     'is_private': '', 'is_professional_account': '', 'is_verified': '',
                                     'profile_pic_url': followee._asdict()['profile_pic_url']})
                        kk += 1
                        if kk == max_count:
                            print('Breaking loop since max_count is reached')
                            break
                    df = pd.json_normalize(data)
                    df.to_csv(instaloader.format_filename_within_target_path(profile.userid.lower(),
                                                                             profile,
                                                                             'profile',
                                                                             'followees',
                                                                             'csv'))
                    followees_flag = False
                    del data

    # except QueryReturnedNotFoundException
        except InstaloaderException as err:
            message = str(err)
            if 'checkpoint' in message.lower():
                print('Probably scraping account was blocked.')
                break
                raise KeyboardInterrupt

        except LoginRequiredException as err:
            message = str(err)
            if message == 'You need to login in order to scrape profile followers and followings!':
                print('Cant proceed without login. Raising KeyboardInterrupt.')
                raise KeyboardInterrupt

        except KeyboardInterrupt:
            print("\nInterrupted by user.", file=sys.stderr)
            raise
    # Save session if it is useful
    if instaloader.context.is_logged_in:
        instaloader.save_session_to_file(sessionfile)