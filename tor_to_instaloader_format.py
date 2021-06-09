import pandas as pd
import glob
import os
import argparse
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('--path', type = str, required = True, help = 'path to where .csv files from tor_scraper are located.')
parser.add_argument('--out_filename', type = str, default = 'scrape_data.csv', help = 'filename of the output csv file.')
args = parser.parse_args()
dfs = []
for item in glob.glob(os.path.join(args.path, '*.csv')):
    try:
        df = pd.read_csv(item, engine = 'python', sep = ',')
        dfs.append(df)
    except KeyboardInterrupt:
        raise
    except:
        print('Error with csv: {}'.format(item))
all_data = pd.concat(dfs)
all_data = all_data.drop_duplicates('id', keep = 'first')
bad_words = ['sale', 'discount', 'offer', 'price', 'ship', 'special']
def remove_bad_posts(df, bad_words):
    idxs = []
    for item in bad_words:
        for idx, row in df.iterrows():
            if item in row['edge_media_to_caption.edges']:
                idxs.append(idx)
    idxs = np.unique(idxs)
    df = df.drop(index = idxs)
    return df

all_data = remove_bad_posts(all_data, bad_words)
ids = np.array(all_data['owner.id'], dtype = str)
new_df = pd.DataFrame(columns = ['id', 'username', 'biography', 'business_category_name', 'connected_fb_page',
                                 'external_url', 'full_name', 'is_business_account', 'is_joined_recently',
                                 'is_private', 'is_professional_account', 'is_verified', 'profile_pic_url'])
new_df['id'] = ids
new_df.to_csv('to_scrape_data.csv', sep = ';', index = None)



