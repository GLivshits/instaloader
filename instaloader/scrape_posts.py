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

def _main(instaloader: Instaloader, targetlist: List[Dict], df, filepath,
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
                if not isinstance(df, type(None)):
                    df.loc[idx, 'downloaded'] = True
                    if k % 100 == 0:
                        df.to_csv(filepath, sep=';', index=None)

    except KeyboardInterrupt:
        if not isinstance(df, type(None)):
            df.to_csv(filepath, sep=';', index=None)
        print("\nInterrupted by user.", file=sys.stderr)
        raise
    except:
        raise

    # Save session if it is useful
    if instaloader.context.is_logged_in:
        instaloader.save_session_to_file(sessionfile)

def main(profiles, filename, **kwargs):

    ids = []
    if not isinstance(filename, type(None)):
        df = pd.read_csv(filename, engine='python', sep=';')
        if 'downloaded' not in df.columns:
            df['downloaded'] = False
        df['id'] = df['id'].astype(str)
        df['username'] = df['username'].astype(str)
        ids = df[['id', 'username']][~df['downloaded']]
        ids['id'] = ids['id'].astype(str)
        ids['username'] = ids['username'].astype(str)
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
                            dirname_pattern='data/{target}', filename_pattern='{target}_{date_utc}',
                            download_pictures = kwargs.get('download_pictures', False),
                            download_videos = kwargs.get('download_videos', False),
                            download_video_thumbnails = kwargs.get('download_video_thumbnails', False),
                            download_geotags = False,
                            download_comments=False,
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
                fast_update=True,
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
