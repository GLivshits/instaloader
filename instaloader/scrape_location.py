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
from .utils import login

def _main(instaloader: Instaloader,
          loc: Dict,
          username: Optional[str] = None,
          password: Optional[str] = None,
          max_count: Optional[int] = 1000000) -> None:

    flag = True
    loc = str(loc['id'])
    login(instaloader, username, password)
    while flag:
        try:
            instaloader.context.log("Scraping location: {}".format(loc))
            instaloader.download_location(loc, max_count = max_count)
            flag = False
        except LoginRequiredException:
            time.sleep(10)
            continue
        except (QueryReturnedNotFoundException, ProfileNotExistsException) as err:
            raise err
        except KeyboardInterrupt as err:
            raise err


