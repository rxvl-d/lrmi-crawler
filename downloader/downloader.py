import argparse
from glob import glob
from multiprocessing import Pool
import os


def argparser():
    parser = argparse.ArgumentParser(
        prog='WDC Downloader',
        description='WDC Downloader')
    parser.add_argument('parallel', type=int, help='how many downloads in parallel')
    return parser


def get_files_to_download():
    with open('file.list') as f:
        urls_to_download = [l.strip() for l in f if l.strip()]
    files_to_download = {u.split('/')[-1]: u for u in urls_to_download}
    already_downloaded = set(glob('*.gz'))
    files_left_to_download = dict()
    for file in files_to_download:
        if file not in already_downloaded:
            files_left_to_download[file] = files_to_download[file]
    print(f"{len(files_left_to_download)}/{len(files_to_download)} left.")
    return list(enumerate(files_left_to_download.items()))


def download_file(index_filename_url):
    index, (filename, url) = index_filename_url
    print(f'Starting [{index}] {url}')
    return_value = os.system(f"wget -q {url} -O {filename}.temp")
    if return_value == 0:
        os.system(f'mv {filename}.temp {filename}')
        print(f'Success [{index}] {url}')
    else:
        print(f'Failure [{index}] {url}')


def main(args):
    n_parallel = args.parallel
    files_to_download = get_files_to_download()
    with Pool(n_parallel) as p:
        p.map(download_file, files_to_download)


if __name__ == '__main__':
    main(argparser().parse_args())
