import json
from io import StringIO
from collections import OrderedDict

import pandas as pd


def read_csv(dataset, csv_path):
    data = dataset
    for level in csv_path.split('/'):
        data = data[level]

    return pd.read_csv(StringIO(data.decode()))


def get_split(ds, ld, label):
    split_index = ds[ds['type'] == label].d3mIndex
    ld_indexed = ld.set_index('d3mIndex')
    split = ld_indexed.loc[split_index]
    return split.reset_index()


def to_csv(df):
    buf = StringIO()
    df.to_csv(buf, index=None)
    return buf.getvalue().encode()


def get_problem_names(dataset, dataset_name):
    problem_names = []
    for key in dataset.keys():
        if dataset_name in key:
            key = key.replace(dataset_name, '').replace('_problem', '')
            if key != '_dataset':
                problem_names.append(key)

    return problem_names


def get_target_names(problem_data, learning_data, dataset_doc):
    target_names = [target['colName'] for target in problem_data['targets']]
    if all(target in learning_data for target in target_names):
        return target_names

    print("WARNING: target names not found in learning data")
    print("Falling back to using suggestedTarget roles")

    def ld_filter(d):
        return d['resPath'] ==  'tables/learningData.csv'

    resources = dataset_doc['dataResources']
    ld_resource = list(filter(lambda d: d['resPath'] == 'tables/learningData.csv', resources))[0]
    target_columns = filter(lambda d: 'suggestedTarget' in d['role'], ld_resource['columns'])

    return [column['colName'] for column in target_columns]


def get_dataset_split(full_dataset, dataset_name, label, problem, targets=False):
    problem_suffix = '_problem' + problem

    # get dataframes
    ds = read_csv(full_dataset, dataset_name + problem_suffix + '/dataSplits.csv')
    ld = read_csv(full_dataset, dataset_name + '_dataset/tables/learningData.csv')

    # get the learningData split
    ld_split = get_split(ds, ld, label)

    # NOTE: Here we use a copy instead of a deepcopy to avoid duplicating
    # all the data inside the tables structure
    # However, we still need to make the tables copy explicit to avoid
    # Overwritting the learningData.csv in the main dataset dict.
    dataset_split = full_dataset[dataset_name + '_dataset'].copy()
    dataset_split['tables'] = dataset_split['tables'].copy()

    suffix = '_' + label + problem

    # Prepare the datasetDoc.json
    dataset_doc = json.loads(dataset_split['datasetDoc.json'].decode(),
                         object_pairs_hook=OrderedDict)
    dataset_id = dataset_doc['about']['datasetID'] + suffix
    dataset_doc['about']['datasetID'] = dataset_id

    # preparo the problemDoc.json
    problem_split = full_dataset[dataset_name + problem_suffix].copy()
    problem_doc = json.loads(problem_split['problemDoc.json'].decode(),
                             object_pairs_hook=OrderedDict)

    problem_doc['about']['problemID'] += suffix
    problem_data = problem_doc['inputs']['data'][0]

    problem_data['datasetID'] = dataset_id

    target_names = get_target_names(problem_data, ld, dataset_doc)

    dataset_split['datasetDoc.json'] = json.dumps(dataset_doc, indent=2).encode()
    problem_split['problemDoc.json'] = json.dumps(problem_doc, indent=2).encode()

    split = {
        'problem_' + label: problem_split,
        'dataset_' + label: dataset_split
    }

    if targets:
        split['targets.csv'] = to_csv(ld_split[['d3mIndex'] + target_names])

    if label == 'TEST':
        for target_name in target_names:
            del ld_split[target_name]

        dataset_split['tables']['learningData.csv'] = to_csv(ld_split)

    return split


def add_dataset_splits(dataset, dataset_name):
    problems = get_problem_names(dataset, dataset_name)
    for problem in problems:
        dataset['TRAIN' + problem] = get_dataset_split(
            dataset, dataset_name, 'TRAIN', problem)
        dataset['TEST' + problem] = get_dataset_split(
            dataset, dataset_name, 'TEST', problem)
        dataset['SCORE' + problem] = get_dataset_split(
            dataset, dataset_name, 'TEST', problem, targets=True)
