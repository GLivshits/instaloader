from typing import Optional

from . import (Instaloader, Profile, ProfileNotExistsException, QueryReturnedNotFoundException)

from .exceptions import LoginRequiredException
import time


def _main(instaloader: Instaloader,
          profile: Profile,
          download_posts = False,
          download_stories: bool = False,
          download_highlights: bool = False,
          download_tagged: bool = False,
          download_igtv: bool = False,
          fast_update: bool = False,
          max_count: Optional[int] = None) -> None:
    flag = True
    while flag:
        try:
            instaloader.download_profiles({profile},
                                          download_posts, download_tagged, download_igtv,
                                          download_highlights, download_stories, max_count=max_count,
                                          fast_update=fast_update, save_profile_metadata = True)
            flag = False
        except LoginRequiredException:
            time.sleep(10)
            continue
        except (QueryReturnedNotFoundException, ProfileNotExistsException):
            raise
        except KeyboardInterrupt:
            raise
