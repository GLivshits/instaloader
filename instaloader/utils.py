import pandas as pd
import re
from operator import itemgetter
from typing import Dict
from .instaloader import Instaloader
from .exceptions import QueryReturnedNotFoundException, ProfileNotExistsException, TwoFactorAuthRequiredException, BadCredentialsException
from .structures import Profile


def get_profile_struct(instaloader: Instaloader,
                       target: Dict):
    user_id = target['id']
    user_name = target['username']
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
                raise err
    else:
        try:
            print('Trying to access profile via id.')
            profile = Profile.from_id(instaloader.context, user_id)
        except (QueryReturnedNotFoundException, ProfileNotExistsException) as err:
            raise err
    return profile

def login(loader: Instaloader, username: str, password: str):
    if username is not None:
        if not re.match(r"^[A-Za-z0-9._]+$", username):
            loader.context.error(
                "Warning: Parameter \"{}\" for --login is not a valid username.".format(username))
        # try:
        #     loader.load_session_from_file(username, sessionfile)
        # except FileNotFoundError as err:
        #     if sessionfile is not None:
        #         print(err, file=sys.stderr)
        #     loader.context.log("Session file does not exist yet - Logging in.")
        if not loader.context.is_logged_in or username != loader.test_login():
            if password is not None:
                try:
                    loader.login(username, password)
                except TwoFactorAuthRequiredException:
                    while True:
                        try:
                            code = input("Enter 2FA verification code: ")
                            loader.two_factor_login(code)
                            break
                        except BadCredentialsException:
                            pass
            else:
                loader.interactive_login(username)
        loader.context.log("Logged in as %s." % username)


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
