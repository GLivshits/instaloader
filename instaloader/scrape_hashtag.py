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

def _main(instaloader: Instaloader, targetlist: List[Dict], df, filepath,
          username: Optional[str] = None, password: Optional[str] = None,
          sessionfile: Optional[str] = None,
          download_profile_pic: bool = True, download_posts=True,
          download_stories: bool = False,
          download_highlights: bool = False,
          download_tagged: bool = False,
          download_igtv: bool = False,
          fast_update: bool = False,
          max_count: Optional[int] = 1000000, post_filter_str: Optional[str] = None,
          storyitem_filter_str: Optional[str] = None, ) -> None:

    k = 1
    try:
        # Generate set of profiles, already downloading non-profile targets
        for hashtag in targetlist:
            flag = True
            while flag:
                try:
                    instaloader.context.log("{}. #{}".format(k, hashtag))
                    instaloader.download_hashtag(hashtag, max_count = max_count, profile_pic = False)
                except LoginRequiredException:
                    time.sleep(10)
                    continue
                k += 1
                flag = False

    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        raise
    except:
        raise

    # Save session if it is useful
    if instaloader.context.is_logged_in:
        instaloader.save_session_to_file(sessionfile)

def main(profiles, filename, **kwargs):

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
                            max_connection_attempts=10,
                            request_timeout=10.0,
                            resume_prefix='iterator',
                            check_resume_bbd=False,
                            rapidapi_key=None, proxyrotator = kwargs.get('proxy_object', None))

    if len(profiles) != 0:
        print('Scraping {} profiles.'.format(len(profiles)))
        _main(loader, profiles, None, filename,
              username=None, password=None,
                sessionfile=None,
                download_profile_pic = kwargs.get('download_profile_pic', False),
                download_posts = True,
                download_stories=False,
                download_highlights=False,
                download_tagged=False,
                download_igtv=False,
                fast_update=True,
                max_count = kwargs.get('max_count', 10000000),
                post_filter_str=None,
                storyitem_filter_str=None)
        loader.close()
    else:
        print('Nothing to scrape here!')
        loader.close()
        raise KeyboardInterrupt


if __name__ == "__main__":
    main()
