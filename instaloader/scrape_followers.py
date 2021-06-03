import os
import re
import sys
import json
from typing import List, Optional, Dict

from . import (Instaloader, InstaloaderException, InvalidArgumentException, Post, Profile, ProfileNotExistsException,
               StoryItem, __version__, load_structure_from_file, TwoFactorAuthRequiredException, ConnectionException,
               BadCredentialsException, QueryReturnedNotFoundException)
from .instaloader import get_default_session_filename
from .instaloadercontext import default_user_agent
from .instaloadercontext import InstaloaderContext
from .exceptions import LoginRequiredException
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

def modify_json(filename, data):
    with open(filename, 'r') as f:
        json_results = json.load(f)
    for item in data:
        json_results.append(item)
    with open(filename, 'w') as f:
        json.dump(json_results, f)

def _main(instaloader: Instaloader, targetlist: List[Dict], df, filepath,
          username: Optional[str] = None, password: Optional[str] = None,
          sessionfile: Optional[str] = None,
          download_profile_pic: bool = True, download_posts=True,
          download_stories: bool = False,
          download_highlights: bool = False,
          download_tagged: bool = False,
          download_igtv: bool = False,
          fast_update: bool = False,
          max_count: Optional[int] = 20000, post_filter_str: Optional[str] = None,
          storyitem_filter_str: Optional[str] = None, scrape_followers: bool = True, scrape_followees: bool = False) -> None:
    count = 1
    kk = 0
    try:
        if username is not None:
            if not re.match(r"^[A-Za-z0-9._]+$", username):
                instaloader.context.error(
                    "Warning: Parameter \"{}\" for --login is not a valid username.".format(username))
            try:
                instaloader.load_session_from_file(username, sessionfile)
            except FileNotFoundError as err:
                if sessionfile is not None:
                    print(err, file=sys.stderr)
                instaloader.context.log("Session file does not exist yet - Logging in.")
            if not instaloader.context.is_logged_in or username != instaloader.test_login():
                if password is not None:
                    try:
                        instaloader.login(username, password)
                    except TwoFactorAuthRequiredException:
                        while True:
                            try:
                                code = input("Enter 2FA verification code: ")
                                instaloader.two_factor_login(code)
                                break
                            except BadCredentialsException:
                                pass
                else:
                    instaloader.interactive_login(username)
            instaloader.context.log("Logged in as %s." % username)

        # Generate set of profiles, already downloading non-profile targets
        for target in targetlist:
            flag = True
            while flag:
                try:
                    user_id = target['id']
                    user_name = target['username']
                    df_row_idx = target.get('idx', None)
                    flag_username_changed = False
                    downloaded_flag = False
                    # If username is present
                    if user_name != 'nan':
                        try:
                            print('Trying to access profile via username.')
                            profile = Profile.from_username(instaloader.context, user_name)
                            downloaded_flag = True
                        except (QueryReturnedNotFoundException, ProfileNotExistsException) as err:
                            print(err)
                            flag_username_changed = True
                            downloaded_flag = False
                    # If username changed somehow
                    if not downloaded_flag:
                        if user_id != 'nan' and flag_username_changed:
                            try:
                                print('Probably username has changed. Trying via id.')
                                profile = Profile.from_id(instaloader.context, user_id)
                                downloaded_flag = True
                            except (QueryReturnedNotFoundException, ProfileNotExistsException) as err:
                                print(err)
                                print('Profile does not exist anymore!')
                                df = delete_row(df, df_row_idx)
                                df.to_csv(filepath, sep=';', index=None)
                                flag = False
                                break
                    # If there was no username but id is present
                        elif (user_id != 'nan') and (not flag_username_changed):
                            try:
                                print('No username was specified. Trying to access profile via id.')
                                profile = Profile.from_id(instaloader.context, user_id)
                                downloaded_flag = True
                            except (QueryReturnedNotFoundException, ProfileNotExistsException) as err:
                                print(err)
                                print('Profile does not exist anymore!')
                                df = delete_row(df, df_row_idx)
                                df.to_csv(filepath, sep=';', index=None)
                                flag = False
                                break
                    # If there was username, but it changed, and there was no id
                        elif user_id == 'nan' and flag_username_changed:
                                print('Profile does not exist anymore!')
                                df = delete_row(df, df_row_idx)
                                df.to_csv(filepath, sep=';', index=None)
                                flag = False
                                break
                    instaloader.context.log("{}. ID:{}, username: {}.".format(count, profile.userid, profile.username))
                    count += 1
                    if instaloader.context.is_logged_in and scrape_followers:
                        data = []
                        json_followers_filename = 'scraped_followers.json'
                        if not os.path.exists(json_followers_filename):
                            with open(json_followers_filename, 'w') as f:
                                json.dump(data, f)
                        data = []
                        print('Scraping followees of user {}.'.format(profile.username))
                        for followee in profile.get_followees():
                            data.append({'id': str(followee._asdict()['id']), 'username': followee._asdict()['username'],
                                         'biography': '', 'business_category_name': '', 'connected_fb_page': '',
                                         'external_url': '', 'full_name': followee._asdict()['full_name'],
                                         'is_business_account': '', 'is_joined_recently': '',
                                         'is_private': '', 'is_professional_account': '', 'is_verified': '',
                                         'profile_pic_url': followee._asdict()['profile_pic_url']})
                            kk += 1
                            # profiles.add(followee)
                            if kk % 100 == 0:
                                print('Downloaded {} users.'.format(kk))
                                modify_json(json_followers_filename, data)
                                data = []
                            if kk == max_count:
                                print('Breaking loop since max_count is reached.')
                                break
                    elif instaloader.context.is_logged_in and scrape_followees:
                        for followee in profile.get_followers():
                            data.append({'id': str(followee._asdict()['id']), 'username': followee._asdict()['username'],
                                         'biography': '', 'business_category_name': '', 'connected_fb_page': '',
                                         'external_url': '', 'full_name': followee._asdict()['full_name'],
                                         'is_business_account': '', 'is_joined_recently': '',
                                         'is_private': '', 'is_professional_account': '', 'is_verified': '',
                                         'profile_pic_url': followee._asdict()['profile_pic_url']})
                            kk += 1
                            # profiles.add(followee)
                            if kk % 100 == 0:
                                print('Downloaded {} users.'.format(kk))
                                modify_json(json_followers_filename, data)
                                data = []
                            if kk == max_count:
                                print('Breaking loop since max_count is reached')
                                break
                        if len(data) > 0:
                            modify_json(json_followers_filename, data)
                    downloaded_flag = True
                    flag = False
                # except QueryReturnedNotFoundException
                except InstaloaderException as err:
                    message = str(err)
                    if 'Checkpoint' in message:
                        print('Probably scraping account was blocked.')
                        break
                        raise KeyboardInterrupt
                flag = False
                if downloaded_flag:
                    df.loc[df_row_idx, 'scraped_followers'] = True
                    df.to_csv(filepath, sep=';', index=None)

    except LoginRequiredException as err:
        message = str(err)
        if message == 'You need to login in order to scrape profile followers and followings!':
            print('Cant proceed without login. Raising KeyboardInterrupt.')
            raise KeyboardInterrupt

    except KeyboardInterrupt:
        df.to_csv(filepath, sep=';', index=None)
        print("\nInterrupted by user.", file=sys.stderr)
        raise

    # Save session if it is useful
    if instaloader.context.is_logged_in:
        instaloader.save_session_to_file(sessionfile)

    raise KeyboardInterrupt

def main(profiles, filename, **kwargs):

    ids = []
    if not isinstance(filename, type(None)):
        df = pd.read_csv(filename, engine='python', sep=';', na_values='nan', dtype='str')
        if 'scraped_followers' not in df.columns:
            df['scraped_followers'] = False
        df['scraped_followers'] = df['scraped_followers'].map({'True': True, 'False': False})
        df = df.drop_duplicates('username', 'last')
        ids = df[['id', 'username']][~df['scraped_followers']]
        ids['idx'] = ids.index
        ids = ids.to_dict('records')
    elif len(profiles) > 0:
        df = None
        for item in profiles:
            if item.isdigit():
                ids.append({'id': item, 'username': ''})
            else:
                ids.append({'id': '', 'username': item})

    loader = Instaloader(sleep=True, quiet=False, user_agent='{}'.format(default_user_agent()),
                            dirname_pattern='results/', filename_pattern='{target}',
                            download_pictures=False,
                            download_videos=False, download_video_thumbnails=True,
                            download_geotags=False,
                            download_comments=False, save_metadata=True,
                            compress_json=False,
                            post_metadata_txt_pattern='',
                            storyitem_metadata_txt_pattern=None,
                            max_connection_attempts=2,
                            request_timeout=60.0,
                            resume_prefix='iterator',
                            check_resume_bbd=False,
                            rapidapi_key=None, proxyrotator=None)
    print('Login is required for scraping followers and followees!')
    username = input('Enter username: ')
    print('/n')
    password = input('Enter password: ')
    print('/n')

    if len(ids) != 0:
        print('Scraping {} profiles.'.format(len(ids)))
        _main(loader, ids, df, filename,
              username=username, password=password,
                sessionfile=None,
                download_profile_pic=False,
                download_posts=False,
                download_stories=False,
                download_highlights=False,
                download_tagged=False,
                download_igtv=False,
                fast_update=False,
                max_count = kwargs.get('max_count', 20000),
                post_filter_str=None,
                storyitem_filter_str=None,
                scrape_followers = kwargs.get('scrape_followers', True),
                scrape_followees = kwargs.get('scrape_followees', False))
        loader.close()
    else:
        print('Nothing to scrape here!')
        loader.close()
        raise KeyboardInterrupt


if __name__ == "__main__":
    main()