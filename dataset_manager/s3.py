import io
import os
import re
import tarfile

import boto3


class S3Manager(object):

    def __init__(self, bucket, root_dir='datasets', skip_sublevels=False):
        self.bucket = bucket
        self.client = boto3.client('s3')
        self.root_dir = root_dir
        self.skip_sublevels = skip_sublevels

    def list_objects(self, prefix=''):
        resp = self.client.list_objects(Bucket=self.bucket, Prefix=prefix)
        keys = [c['Key'] for c in resp.get('Contents', [])]
        while resp['IsTruncated']:
            marker = keys[-1]
            resp = self.client.list_objects(Bucket=self.bucket, Prefix=prefix, Marker=marker)
            keys.extend([c['Key'] for c in resp.get('Contents', [])])

        return keys

    def load_tar(self, dataset_name, raw, tf):
        files = tf.getnames()
        if raw:
            dataset = os.path.join(dataset_name, dataset_name + '_dataset')
            problem = os.path.join(dataset_name, dataset_name + '_problem')
            if self.skip_sublevels:
                prefixes = [dataset + '/tables/', dataset + '/datasetDoc.json', problem]

            else:
                prefixes = [dataset, problem]

            files = [fn for fn in files if any(fn.startswith(prefix) for prefix in prefixes)]

        root = dict()
        for key in files:
            print("Getting file {} from tarfile".format(key))
            with tf.extractfile(key) as buf:
                content = buf.read()

            path, filename = tuple(key.rsplit('/', 1))
            data = root
            for level in path.split('/')[1:]:
                data = data.setdefault(level, dict())

            data[filename] = content

        return root

    def load(self, dataset_name, raw=False):
        key = '{}/{}.tar.gz'.format(self.root_dir, dataset_name)
        print("Getting file {} from bucket {}".format(key, self.bucket))
        content = self.client.get_object(Bucket=self.bucket, Key=key)
        bytes_io = io.BytesIO(content['Body'].read())

        with tarfile.open(fileobj=bytes_io, mode='r:gz') as tf:
            return self.load_tar(dataset_name, raw, tf)

    def write(self, dataset, base_dir):
        bytes_io = io.BytesIO()
        with tarfile.open(fileobj=bytes_io, mode='w:gz') as tf:
            self.write_tar(dataset, base_dir, tf)

        key = '{}/{}.tar.gz'.format(self.root_dir, base_dir)
        print("Writing file {} into S3 bucket {}".format(key, self.bucket))
        self.client.put_object(Bucket=self.bucket, Key=key, Body=bytes_io.getvalue())

    def write_tar(self, dataset, base_dir, tf):
        for path, value in dataset.items():
            key = os.path.join(base_dir, path)
            if isinstance(value, dict):
                self.write_tar(value, key, tf)

            else:
                print("Adding file {} into tarfile".format(key))
                info = tarfile.TarInfo(name=key)
                info.size = len(value)
                bytes_io = io.BytesIO(value)
                tf.addfile(info, bytes_io)

    def datasets(self):
        resp = self.client.list_objects(Bucket=self.bucket, Prefix=self.root_dir)
        names = []
        regex = re.compile('{}/(.+)\.tar\.gz'.format(self.root_dir))
        for entry in resp.get('Contents', []):
            key = entry['Key']
            match = regex.match(key)
            if not match:
                print('WARNING: Invalid dataset name found in S3 bucket {}: {}'.format(
                    self.bucket, key))
            else:
                names.append(match.group(1))

        return names

    def exists(self, dataset_name):
        prefix = '{}/{}.tar.gz'.format(self.root_dir, dataset_name)
        return 'Contents' in self.client.list_objects(Bucket=self.bucket, Prefix=prefix)
