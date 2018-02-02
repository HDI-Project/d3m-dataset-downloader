import os


class LocalManager(object):

    def __init__(self, datasets_path, skip_sublevels=False):
        self.datasets_path = datasets_path
        self.skip_sublevels = skip_sublevels

    @classmethod
    def load_folder(cls, folder, prefixes):
        data = dict()
        for name in os.listdir(folder):
            path = os.path.join(folder, name)
            if any(prefix in path or path in prefix for prefix in prefixes):
                if os.path.isdir(path):
                    data[name] = cls.load_folder(path, prefixes)

                else:
                    with open(path, 'rb') as f:
                        data[name] = f.read()

        return data

    def load(self, dataset_name, raw=False):
        dataset_path = os.path.join(self.datasets_path, dataset_name)
        if raw:
            problem = dataset_name + '_problem'
            problems = [name for name in os.listdir(dataset_path) if problem in name]
            dataset = dataset_name + '_dataset'

            if self.skip_sublevels:
                # restrict the dataset sublevels to datasetDoc.json and tables
                dataset_sublevels = [
                    os.path.join(dataset, 'tables'),
                    os.path.join(dataset, 'datasetDoc.json')
                ]
                prefixes = problems + dataset_sublevels

            else:
                prefixes = problems + [dataset]

        else:
            prefixes = os.listdir(dataset_path)

        prefixes = [os.path.join(dataset_path, prefix) for prefix in prefixes]

        data = self.load_folder(dataset_path, prefixes)

        return data

        return {
            folder: self.load_folder(os.path.join(dataset_path, folder))
            for folder in folders
        }

    def write(self, dataset, base_dir=''):
        full_base_dir = os.path.join(self.datasets_path, base_dir)
        if not os.path.exists(full_base_dir):
            os.makedirs(full_base_dir)

        for key, value in dataset.items():
            path = os.path.join(base_dir, key)
            if isinstance(value, dict):
                self.write(value, path)

            else:
                path = os.path.join(self.datasets_path, path)
                print("Writing file {}".format(path))
                with open(path, 'wb') as f:
                    f.write(value)

    def datasets(self):
        return list(sorted(os.listdir(self.datasets_path)))

    def exists(self, dataset_name):
        dataset_path = os.path.join(self.datasets_path, dataset_name)
        return os.path.exists(dataset_path)
