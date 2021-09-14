from typing import Optional, Dict
from . import Instaloader, ProfileNotExistsException, QueryReturnedNotFoundException
from .exceptions import LoginRequiredException
import time

def _main(instaloader: Instaloader,
          hashtag: Dict,
          max_count: Optional[int] = 1000000) -> None:

    flag = True
    hashtag = hashtag['username']
    instaloader.context.log("Downloading #{}".format(hashtag))
    while flag:
        try:
            instaloader.download_hashtag(hashtag, max_count = max_count)
            flag = False
        except LoginRequiredException:
            time.sleep(10)
            continue
        except (QueryReturnedNotFoundException, ProfileNotExistsException):
            raise
        except KeyboardInterrupt:
            raise


