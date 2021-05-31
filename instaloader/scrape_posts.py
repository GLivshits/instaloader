"""Download pictures (or videos) along with their captions and other metadata from Instagram."""

import ast
import datetime
import os
import re
import sys
import json
from argparse import ArgumentParser, SUPPRESS
from typing import List, Optional

from . import (Instaloader, InstaloaderException, InvalidArgumentException, Post, Profile, ProfileNotExistsException,
               StoryItem, __version__, load_structure_from_file, TwoFactorAuthRequiredException, ConnectionException,
               BadCredentialsException, QueryReturnedNotFoundException)
from .instaloader import get_default_session_filename
from .instaloadercontext import default_user_agent
from .instaloadercontext import InstaloaderContext
from fake_useragent import UserAgent
from .exceptions import LoginRequiredException
import numpy as np
import requests
import time
import inspect
import pandas as pd
from operator import itemgetter
from requests.exceptions import RequestException

def usage_string():
    # NOTE: duplicated in README.rst and docs/index.rst
    argv0 = os.path.basename(sys.argv[0])
    argv0 = "instaloader" if argv0 == "__main__.py" else argv0
    return """
{0} [--comments] [--geotags]
{2:{1}} [--stories] [--highlights] [--tagged] [--igtv]
{2:{1}} [--login YOUR-USERNAME] [--fast-update]
{2:{1}} profile | "#hashtag" | %%location_id | :stories | :feed | :saved
{0} --help""".format(argv0, len(argv0), '')

def append_new_user(df, user_json):
    profile_id = user_json['id']
    if len(df.query('id == "{}"'.format(profile_id))) >= 1:
        return df
    to_append_list = ['' for _ in range(len(df.columns))]
    to_append_list[0] = profile_id
    to_append_list[1] = user_json['username']
    df = df.append(pd.Series(data = to_append_list, index = df.columns), ignore_index = True)
    return df

def delete_row(df, profile_id):
    df = df.drop(df.query('id == "{}"'.format(profile_id)).index)
    return df

def modify_row(df, json_dict):
    profile_id = json_dict['node']['id']
    query_len = len(df.query('id == "{}"'.format(profile_id)))
    values = itemgetter(*df.columns)(json_dict['node'])
    values = list(values)
    values[-1] = json_dict['node']['profile_pic_url_hd']

    if query_len >= 1:
        # print('This profile already in table.')
        df.loc[df.query('id == "{}"'.format(profile_id)).index, :] = values
        return df
    else:
        # print('Appending new profile.')
        df = df.append(pd.Series(data = values, index = df.columns), ignore_index = True)
        return df

def _main(instaloader: Instaloader, targetlist: List[str], df, filepath,
          username: Optional[str] = None, password: Optional[str] = None,
          sessionfile: Optional[str] = None,
          download_profile_pic: bool = True, download_posts=True,
          download_stories: bool = False,
          download_highlights: bool = False,
          download_tagged: bool = False,
          download_igtv: bool = False,
          fast_update: bool = False,
          max_count: Optional[int] = None, post_filter_str: Optional[str] = None,
          storyitem_filter_str: Optional[str] = None, ) -> None:

    k = 1
    try:
        # Generate set of profiles, already downloading non-profile targets
        for target in targetlist:
            flag = True
            while flag:
                try:
                    user_id = target['id']
                    user_name = target['username']
                    # print(user_id, user_name)
                    if user_name != 'nan':
                        try:
                            print('Trying to access profile via username.')
                            profile = Profile.from_username(instaloader.context, user_name)
                        except (QueryReturnedNotFoundException, ProfileNotExistsException) as err:
                            print(err)
                            print('Probably username has changed. Trying via id.')
                            try:
                                profile = Profile.from_id(instaloader.context, user_id)
                            except (QueryReturnedNotFoundException, ProfileNotExistsException) as err:
                                print(err)
                                print('Profile does not exist anymore!')
                                df = delete_row(df, user_id)
                                df.to_csv(filepath, sep=';', index=None)
                                flag = False
                                break
                    else:
                        try:
                            print('Trying to access profile via id.')
                            profile = Profile.from_id(instaloader.context, user_id)
                        except (QueryReturnedNotFoundException, ProfileNotExistsException) as err:
                            print(err)
                            print('Profile does not exist anymore!')
                            df = delete_row(df, user_id)
                            df.to_csv(filepath, sep=';', index=None)
                            flag = False
                            break

                    instaloader.context.log("{}. ID:{}, username: {}.".format(k, profile.userid, profile.username))
                    instaloader.download_profiles({profile},
                                                  download_profile_pic, download_posts, download_tagged, download_igtv,
                                                  download_highlights, download_stories, max_count = max_count,
                                                  fast_update = fast_update, download_metadata = True)
                except LoginRequiredException:
                    time.sleep(10)
                    continue
                k += 1
                flag = False
                # with open(os.path.join('results', '{}-{}.json'.format(profile.username, profile.userid)), 'r') as f:
                #     json_dict = json.load(f)
                df.loc[df.query('id == "{}"'.format(user_id)).index, 'downloaded'] = True
                if k % 100 == 0:
                    df.to_csv(filepath, sep=';', index=None)

    except KeyboardInterrupt:
        df.to_csv(filepath, sep=';', index=None)
        print("\nInterrupted by user.", file=sys.stderr)
        raise
    except:
        raise

    # Save session if it is useful
    if instaloader.context.is_logged_in:
        instaloader.save_session_to_file(sessionfile)

def main(filename, **kwargs):

    df = pd.read_csv(filename, engine='python', sep=';')
    if 'downloaded' not in df.columns:
        df['downloaded'] = False
    df['id'] = df['id'].astype(str)
    df['username'] = df['username'].astype(str)
    ids = df[['id', 'username']][~df['downloaded']]
    ids['id'] = ids['id'].astype(str)
    ids['username'] = ids['username'].astype(str)
    # print(ids)
    ids = ids.to_dict('records')

    loader = Instaloader(sleep=True, quiet=False, user_agent='{}'.format(default_user_agent()),
                            dirname_pattern='data/{target}', filename_pattern='{target}_{date_utc}',
                            download_pictures = kwargs.get('download_pictures', False),
                            download_videos = kwargs.get('download_videos', False),
                            download_video_thumbnails = kwargs.get('download_video_thumbnails', False),
                            save_metadata = kwargs.get('save_metadata', True),
                            compress_json = kwargs.get('compress_json', False),
                            post_metadata_txt_pattern='',
                            storyitem_metadata_txt_pattern=None,
                            max_connection_attempts=2,
                            request_timeout=5.0,
                            resume_prefix='iterator',
                            check_resume_bbd=False,
                            rapidapi_key=None, proxyrotator = kwargs.get('proxy_object', None))

    if len(ids) != 0:
        print('Scraping {} profiles.'.format(len(ids)))
        _main(loader, ids, df, filename,
              username=None, password=None,
                sessionfile=None,
                download_profile_pic = kwargs.get('download_profile_pic', False),
                download_posts = True,
                download_stories=False,
                download_highlights=False,
                download_tagged=False,
                download_igtv=False,
                fast_update=False,
                max_count = kwargs.get('max_count', 1000),
                post_filter_str=None,
                storyitem_filter_str=None)
        loader.close()
    else:
        print('Nothing to scrape here!')
        loader.close()
        raise KeyboardInterrupt


if __name__ == "__main__":
    main()
