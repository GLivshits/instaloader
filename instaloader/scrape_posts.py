import sys
from typing import List, Optional, Dict
from . import (Instaloader, InstaloaderException, InvalidArgumentException, Post, Profile, ProfileNotExistsException,
               StoryItem, __version__, load_structure_from_file, TwoFactorAuthRequiredException, ConnectionException,
               BadCredentialsException, QueryReturnedNotFoundException)
from .instaloadercontext import default_user_agent
from .exceptions import LoginRequiredException
import time
import pandas as pd
from operator import itemgetter

def append_new_user(df, user_json):
    profile_id = user_json['id']
    if len(df.query('id == "{}"'.format(profile_id))) >= 1:
        return df
    to_append_list = ['' for _ in range(len(df.columns))]
    to_append_list[0] = profile_id
    to_append_list[1] = user_json['username']
    df = df.append(pd.Series(data = to_append_list, index = df.columns), ignore_index = True)
    return df

def delete_row(df, row_idx):
    df = df.drop(row_idx)
    return df

def modify_row(df, json_dict, row_idx):
    items = df.columns.tolist()
    items[-1] = 'profile_pic_url_hd'
    values = itemgetter(*items)(json_dict['node'])
    values = list(values)
    df.loc[row_idx, :] = values
    return df

def _main(instaloader: Instaloader, target: [Dict],
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

    flag = True
    while flag:
        try:
            user_id = target['id']
            user_name = target['username']
            idx = target.get('idx', None)
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
                        raise
            else:
                try:
                    print('Trying to access profile via id.')
                    profile = Profile.from_id(instaloader.context, user_id)
                except (QueryReturnedNotFoundException, ProfileNotExistsException) as err:
                    raise
            instaloader.download_profiles({profile},
                                          download_profile_pic, download_posts, download_tagged, download_igtv,
                                          download_highlights, download_stories, max_count = max_count,
                                          fast_update = fast_update, download_metadata = True)
            flag = False
        except LoginRequiredException:
            time.sleep(10)
            continue
        except (QueryReturnedNotFoundException, ProfileNotExistsException):
            raise
        except KeyboardInterrupt:
            raise
