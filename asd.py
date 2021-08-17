import os
import shutil
from tqdm import tqdm

path = '/home/grisha/4TB/data'
for item in tqdm(os.listdir(path)):
    full_path = os.path.join(path, item)
    if 'posts' not in os.listdir(full_path):
        shutil.rmtree(full_path)
