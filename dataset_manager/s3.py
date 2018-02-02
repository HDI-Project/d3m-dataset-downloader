import os

import boto3


class S3Manager(object):

    def __init__(self, bucket, skip_sublevels=False):
        self.bucket = bucket
        self.client = boto3.client('s3')
        self.skip_sublevels = skip_sublevels

    def list_objects(self, prefix=''):
        resp = self.client.list_objects(Bucket=self.bucket, Prefix=prefix)
        keys = [c['Key'] for c in resp.get('Contents', [])]
        while resp['IsTruncated']:
            marker = keys[-1]
            resp = self.client.list_objects(Bucket=self.bucket, Prefix=prefix, Marker=marker)
            keys.extend([c['Key'] for c in resp.get('Contents', [])])

        return keys

    def load(self, dataset_name, raw=False):
        if raw:
            dataset = os.path.join(dataset_name, dataset_name + '_dataset')
            if self.skip_sublevels:
                files = self.list_objects(dataset + '/tables/')
                files.extend(self.list_objects(dataset + '/datasetDoc.json'))
            else:
                files = self.list_objects(dataset)

            problem = os.path.join(dataset_name, dataset_name + '_problem')
            files.extend(self.list_objects(problem))

        else:
            files = self.list_objects(dataset_name)

        root = dict()
        for key in files:
            print("Getting file {} from bucket {}".format(key, self.bucket))
            content = self.client.get_object(Bucket=self.bucket, Key=key)

            path, filename = tuple(key.rsplit('/', 1))
            data = root
            for level in path.split('/')[1:]:
                data = data.setdefault(level, dict())

            data[filename] = content['Body'].read()

        return root

    def write(self, dataset, base_dir=''):
        for path, value in dataset.items():
            key = os.path.join(base_dir, path)
            if isinstance(value, dict):
                self.write(value, key)

            else:
                print("Saving file {} into s3 bucket {}".format(key, self.bucket))
                self.client.put_object(Bucket=self.bucket, Key=key, Body=value)

    def datasets(self):
        resp = self.client.list_objects(Bucket=self.bucket, Delimiter='/')
        names = [cp['Prefix'] for cp in resp.get('CommonPrefixes', [])]
        return [name[:-1] for name in names if name[-1] == '/']

    def exists(self, dataset_name):
        return 'Contents' in self.client.list_objects(Bucket=self.bucket, Prefix=dataset_name)
