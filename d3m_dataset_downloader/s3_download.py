import argparse
import os

import boto3


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='S3 dataset downloader')
    parser.add_argument('dataset', type=str, help='id of dataset')
    parser.add_argument('-o', '--output', type=str, help='output folder')
    parser.add_argument('-b', '--bucket', type=str, default='d3m-data-dai',
                        nargs='?', help='Bucket name')

    args = parser.parse_args()

    client = boto3.client('s3')

    resp = client.list_objects(Bucket=args.bucket, Prefix=args.dataset)

    for entry in resp.get('Contents', []):
        key = entry['Key']
        path, filename = tuple(key.rsplit('/', 1))
        local_path = os.path.join(args.output, path)
        if path and not os.path.exists(local_path):
            os.makedirs(local_path)

        content = client.get_object(Bucket=args.bucket, Key=key)

        local_filename = os.path.join(args.output, key)
        print("Writing {}".format(local_filename))
        with open(local_filename, 'wb') as f:
            f.write(content['Body'].read())
