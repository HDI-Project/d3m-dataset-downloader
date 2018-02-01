import argparse
import os

import boto3


def upload_dataset(client, bucket, input_dir, dataset, files):
    print("Cleaning up dataset {}".format(dataset))
    cleanup(client, bucket, dataset)
    for filename in files:
        local_filename = os.path.join(input_dir, filename)
        print("Uploading file {} into s3:{}:{}".format(
            local_filename, bucket, filename))
        client.upload_file(local_filename, bucket, filename)


def cleanup(client, bucket, dataset):
    """Delete existing dataset objects."""
    resp = client.list_objects(Bucket=bucket, Prefix=dataset + '/')
    for obj in resp.get('Contents', []):
        client.delete_object(Bucket=bucket, Key=obj['Key'])


def check_write(client, bucket, dataset, skip):
    if 'Contents' in client.list_objects(Bucket=bucket, Prefix=dataset + '/'):
        if skip:
            return False

        print("Dataset {} already exists in bucket {}".format(dataset, bucket))
        ow = input("Overwrite (y/n)? ")
        while ow not in ('y', 'n'):
            print("Please insert y or n")
            ow = input("Overwrite (y/n)? ")

        return ow == 'y'

    return True


def find_files(path, base_dir):
    files = []
    for name in os.listdir(os.path.join(base_dir, path)):
        if os.path.isdir(os.path.join(base_dir, path, name)):
            files.extend(find_files(os.path.join(path, name), base_dir))
        else:
            files.append(os.path.join(path, name))

    return files


def load_datasets(args):
    invalid = []
    datasets = dict()
    for dataset_name in args.dataset:
        dataset_path = os.path.join(args.input, dataset_name)
        if not os.path.exists(dataset_path):
            invalid.append(dataset_name)
        else:
            datasets[dataset_name] = find_files(dataset_name, args.input)

    return datasets, invalid


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='S3 dataset downloader')
    parser.add_argument('-i', '--input', default='.', nargs='?', help='input folder')
    parser.add_argument('-b', '--bucket', type=str, default='d3m-data-dai',
                        nargs='?', help='Bucket name')

    force_skip_group = parser.add_mutually_exclusive_group()
    force_skip_group.add_argument('-f', '--force', action='store_true', help='Overwrite without asking')
    force_skip_group.add_argument('-s', '--skip', action='store_true', help='Skip existing datasets')

    parser.add_argument('dataset', nargs='+', help='id of the datasets to upload')

    args = parser.parse_args()

    datasets, invalid = load_datasets(args)

    if invalid:
        print("Invalida dataset names: {}".format(invalid))

    else:
        client = boto3.client('s3')

        for dataset, files in datasets.items():
            if args.force or check_write(client, args.bucket, dataset, args.skip):
                upload_dataset(client, args.bucket, args.input, dataset, files)
