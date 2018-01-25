import requests
import os
from bs4 import BeautifulSoup
import json
import subprocess
import shutil

COHORT_URLS = {
    'seed': "https://datadrivendiscovery.org/data/seed_datasets_current/",
    'll0': "https://datadrivendiscovery.org/data/training_datasets/LL0/",
    'll1': "https://datadrivendiscovery.org/data/training_datasets/LL1/"
}

def get_page(url):
    return requests.get(
        url, headers={'Authorization': os.environ['TOKEN']})


def make_dataset_doc_link(root, name):
    return os.path.join(root,name, "{name}_dataset/datasetDoc.json".format(name=name))


def save_dataset_docs(ds_cohort, output_file):
    root = COHORT_URLS[ds_cohort]
    response = get_page(root)
    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all('li')
    datasets = {}
    for l in links:
        dataset_name = l.a.get('href').replace('/', '')
        dataset_doc_link = make_dataset_doc_link(root, dataset_name)
        print(dataset_doc_link)
        try:
            dataset_doc = get_page(dataset_doc_link).json()
        except:
            continue
        else:
            datasets[dataset_name] = dataset_doc
    with open(output_file, 'w') as d:
        json.dump(datasets, d)

# save_dataset_docs(ll0_training_datasets_url, 'll0_dataset_docs.json')
# save_dataset_docs(seed_dataset_url, 'seed_dataset_docs.json')

def download_dataset_from_name(name, output_dir):
    ds_cohort = 'seed'
    if name.lower().startswith('ll0'):
        ds_cohort = 'll0'
    elif name.lower().startswith('ll1'):
        ds_cohort = 'll1'
    root = COHORT_URLS[ds_cohort]
    if os.path.exists(output_dir):
        replace = input("{} exists, remove and replace it? y/n".format(output_dir))
        if replace:
            shutil.rmtree(output_dir)
    url = os.path.join(root, name) + '/'
    token = os.environ['TOKEN']
    download_cmd = [
    'wget', '-q', '-r', '-np', '-R',
    'index.html*', '-nH', '--header',
    "Authorization:{}".format(token),
    url
    ]
    subprocess.run(download_cmd)
    root_on_disk = root.replace('https://datadrivendiscovery.org/', '')

    shutil.move(os.path.join(root_on_disk, name), output_dir)
    shutil.rmtree('data')


def check_type(dataset, dtype):
    if dtype == 'tabular':
        return check_tabular(dataset)
    for res in dataset['dataResources']:
        if dtype == res['resType']:
            return True


def check_tabular(dataset):
    all_table = True
    for res in dataset['dataResources']:
        if res['resType'] != 'table':
            all_table = False
            break
    return all_table


def download_dataset_from_type(types_doc, ds_cohort, dtype, output_dir,
                               max_datasets=None):
    root = COHORT_URLS[ds_cohort]
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    downloaded = 0
    with open(types_doc) as f:
        d = json.load(f)
        for name, dataset in d.items():
            if check_type(dataset, dtype):
                print("Downloading {}".format(name))
                download_dataset_from_name(name, os.path.join(output_dir, name))
                downloaded += 1
                if downloaded >= max_datasets:
                    return
