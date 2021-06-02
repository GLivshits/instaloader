import argparse
import os
import json
import lzma
import concurrent.futures
import gc

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
    data_json = []
    data_path = os.path.join(path, 'posts')
    target_keys = ['__typename', 'id', 'shortcode', 'dimensions', 'display_url', 'is_video', 'accessibility_caption',
                   'taken_at_timestamp', 'thumbnail_resources']
    if os.path.exists(data_path):
        for item in os.listdir(data_path):
            json_path = os.path.join(data_path, item)
            open_func = open
            if item.endswith('.xz'):
                open_func = lzma.open
            with open_func(json_path, 'r') as f:
                file = json.load(f)
            to_append = dict(zip(target_keys, itemgetter(*target_keys)(file['node'])))
            data_json.append(to_append)
    with lzma.open(os.path.join(path, 'all_posts.json.xz'), 'wt') as f:
        json.dump(data_json, f)

def main():
    parser = argparse.ArgumentParser(description = 'Args for merging jsons.')
    parser.add_argument('--path', type = str, required = True, help = 'Path to folder with all profiles (each profile as unique folder).')
    parser.add_argument('--num-workers', type = int, default = None, help = 'How many cores to utilize.')
    args = parser.parse_args()
    all_paths = list(map(lambda x: os.path.join(args.path, x), os.listdir(args.path)))
    with concurrent.futures.ProcessPoolExecutor(max_workers = args.num_workers) as executor:
        executor.map(merge_json, all_paths)

if __name__ == '__main__':
    main()


