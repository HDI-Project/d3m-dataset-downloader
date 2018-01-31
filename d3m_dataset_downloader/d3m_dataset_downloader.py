import os
from argparse import ArgumentParser
from getpass import getpass

import boto3

from scraper import Scraper
from splitter import add_dataset_splits


def save_dataset(dataset, base_dir, s3_client=None, s3_bucket=None):
    if not s3_client and not os.path.exists(base_dir):
        os.makedirs(base_dir)

    for key, value in dataset.items():
        path = os.path.join(base_dir, key)
        if isinstance(value, dict):
            save_dataset(value, path, s3_client, s3_bucket)

        elif s3_client:
            print("Saving file {} into s3 bucket {}".format(path, s3_bucket))
            s3_client.put_object(Bucket=s3_bucket, Key=path, Body=value)

        else:
            print("Writting file {}".format(path))
            with open(path, 'wb') as f:
                f.write(value)


def downloaded(dataset_name, output, s3_client):
    if s3_client:
        return 'Contents' in s3_client.list_objects(Bucket=output, Prefix=dataset_name)

    else:
        dataset_path = os.path.join(output, dataset_name)
        return os.path.exists(dataset_path)


def get_dataset(scraper, dataset_name, output, s3_client, force):
    if force or not downloaded(dataset_name, output, s3_client):
        print("Downloading Dataset {}".format(dataset_name))
        dataset = scraper.scrape_dataset(dataset_name)
        add_dataset_splits(dataset, dataset_name)

        if s3_client:
            s3_bucket = output
            base_dir = dataset_name
        else:
            s3_bucket = None
            base_dir = os.path.join(output, dataset_name)

        save_dataset(dataset, base_dir, s3_client, s3_bucket)

    else:
        print("Dataset {} found in output folder. Skipping...".format(dataset_name))


if __name__ == '__main__':

    parser = ArgumentParser(description='D3M dataset downloader')
    parser.add_argument('dataset', type=str, nargs='*', help='id of dataset')
    parser.add_argument('-u', '--username', type=str, help='D3M Username')
    parser.add_argument('-p', '--password', type=str, help='D3M Password')
    parser.add_argument('-d', '--delay', type=int, default=0,
                        help='Seconds to wait between requests')
    parser.add_argument('-s', '--skip-sublevels', action='store_true',
                        help='Skip dataset sublevels. For debug purposes')
    parser.add_argument('-l', '--list', nargs='?', const='seed,ll0,ll1',
                        help=('List all Datasets of the given type, '
                              'multiple comma-separated values accepted'))
    parser.add_argument('-a', '--all', nargs='?', const='seed,ll0,ll1',
                        help=('Get all Datasets of the given type, '
                              'multiple comma-separated values accepted'))
    parser.add_argument('-o', '--output', default='datasets', help='Output folder or s3:bucket')
    parser.add_argument('-f', '--force', action='store_true',
                        help='Overwrite previously downloaded datasets')
    parser.add_argument('--skip', default='', required=False)

    args = parser.parse_args()

    if not args.username:
        args.username = input('Username: ')

    if not args.password:
        args.password = getpass()

    scraper = Scraper(args.username, args.password, args.delay, not args.skip_sublevels)
    output = args.output
    s3_client = None

    if output[0:3] == 's3:':
        s3_client = boto3.client('s3')
        output = output[3:]

    if args.list:
        for dataset_type in args.list.split(','):
            for dataset in scraper.get_datasets(dataset_type):
                print(dataset_type, dataset)

    elif args.all:
        for dataset_type in args.all.split(','):
            for dataset in scraper.get_datasets(dataset_type):
                if dataset not in args.skip.split(','):
                    get_dataset(scraper, dataset, output, s3_client, args.force)

    elif args.dataset:
        for dataset in args.dataset:
            get_dataset(scraper, dataset, output, s3_client, args.force)

    else:
        parser.print_help()
