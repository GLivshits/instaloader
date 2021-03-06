import argparse
import os
import json
import lzma
import concurrent.futures
from tqdm import tqdm

def itemgetter(*items, default = None):
    if len(items) == 1:
        item = items[0]
        def g(obj):
            return obj.get(item, default)
    else:
        def g(obj):
            return tuple(obj.get(item, default) for item in items)
    return g

def merge_json(path):
    if os.path.exists(os.path.join(path, 'all_posts.json.xz')):
        return
    data_json = []
    i = 0
    data_path = os.path.join(path, 'posts')
    target_keys = ['__typename', 'id', 'shortcode', 'edge_sidecar_to_children', 'dimensions', 'display_url', 'is_video', 'accessibility_caption',
                   'taken_at_timestamp', 'thumbnail_resources']
    if os.path.exists(data_path):
        for item in os.listdir(data_path):
            i += 1
            json_path = os.path.join(data_path, item)
            open_func = open
            if item.endswith('.xz'):
                open_func = lzma.open
            with open_func(json_path, 'r') as f:
                file = json.load(f)
            to_append = dict(zip(target_keys, itemgetter(*target_keys)(file['node'])))
            data_json.append(to_append)
            if i > 30:
                break
    with lzma.open(os.path.join(path, 'all_posts.json.xz'), 'wt') as f:
        json.dump(data_json, f)

def main():
    parser = argparse.ArgumentParser(description = 'Args for merging jsons.')
    parser.add_argument('--path', type = str, required = True, help = 'Path to folder with all profiles (each profile as unique folder).')
    parser.add_argument('--num-workers', type = int, default = None, help = 'How many cores to utilize.')
    args = parser.parse_args()
    all_paths = list(map(lambda x: os.path.join(args.path, x), os.listdir(args.path)))
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=args.num_workers)
    jobs = [executor.submit(merge_json, item) for item in all_paths]
    i = 0
    for _ in tqdm(concurrent.futures.as_completed(jobs), total=len(jobs)):
        i += 1

if __name__ == '__main__':
    main()
