import json
import time

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class BaseScraper(object):

    PATH = {
        'seed': 'seed_datasets_current/',
        'll0': 'training_datasets/LL0/',
        'll1': 'training_datasets/LL1/'
    }
    DATASET_TYPES = dict()

    STATUS_FORCELIST = (403, 404, 500, 502, 504)
    RETRIES = 10
    BACKOFF_FACTOR = 0.5

    def get_session(self):
        session = requests.Session()
        retry = Retry(self.RETRIES, backoff_factor=self.BACKOFF_FACTOR,
                      status_forcelist=self.STATUS_FORCELIST)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def __init__(self, skip_sublevels=False):
        self.session = self.get_session()
        self.sublevels = ['tables/'] if skip_sublevels else []

    def get_url(self, url, raw=False):
        print("Getting URL {}".format(url))
        r = self.session.get(url)
        if raw:
            return r.content

        else:
            return BeautifulSoup(r.text, 'html.parser')

    @staticmethod
    def get_links(soup):
        links = soup.find_all('a')
        links = [a.text for a in links if '..' not in a.text]
        return [link[:-1] if link[-1] == '/' else link for link in links]

    def get_datasets(self, dataset_type):
        url = self.BASE_URL + self.PATH[dataset_type]
        soup = self.get_url(url)
        links = self.get_links(soup)
        return [link for link in links if '.' not in link]

    def get_sublevel(self, base_path, level, sublevels):
        data = dict()

        if sublevels and level not in sublevels:
            return data

        level_url = base_path + level + '/'
        soup = self.get_url(level_url)

        links = self.get_links(soup)
        for link in links:
            if '.' not in link:
                # This is a link to a subfolder
                if link[-1] == '/':
                    link = link[:-1]

                data[link] = self.get_sublevel(level_url, link, sublevels)
            else:
                data[link] = self.get_url(level_url + link, raw=True)

        return data

    def get_base_url(self, dataset_name):
        if not self.DATASET_TYPES:
            for dataset_type, path in self.PATH.items():
                datasets = self.get_datasets(dataset_type)
                for dataset in datasets:
                    self.DATASET_TYPES[dataset] = self.BASE_URL + path

        return self.DATASET_TYPES[dataset_name]

    def scrape_dataset(self, dataset):
        base_url = self.get_base_url(dataset) + dataset + '/'
        root_links = [
            link for link in self.get_links(self.get_url(base_url))
            if dataset + '_dataset' in link or dataset + '_problem' in link
        ]
        sublevels = self.sublevels.copy()
        if sublevels:
            sublevels.extend(root_links)

        return {
            link: self.get_sublevel(base_url, link, sublevels) for link in root_links
        }


class D3MScraper(BaseScraper):

    BASE_URL = 'https://datadrivendiscovery.org/data/'

    def get_session(self, username, password):
        session = requests.Session()
        retry = Retry(self.RETRIES, backoff_factor=self.BACKOFF_FACTOR)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        login_data = {
            'username': username,
            'password': password,
            'target': '/data'
        }
        session.post('https://datadrivendiscovery.org/login', data=login_data)
        return session

    def __init__(self, username, password, skip_sublevels=False):
        self.session = self.get_session(username, password)
        self.sublevels = ['tables/'] if skip_sublevels else []


class IPFSScraper(BaseScraper):
    BASE_URL = 'https://gateway.ipfs.io/ipfs/QmWsbzjogZTY3Laf8SErQ9azfuY7BWicBmQjP9SxwvtqTz/'


class BaseManager(object):

    def __init__(self, source='D3M', username=None, password=None, skip_sublevels=False):
        self.scraper = D3MScraper(username, password, skip_sublevels)
        self.scraper = IPFSScraper(skip_sublevels)

    def load(self, dataset_name, raw='to_be_ignored'):
        return self.scraper.scrape_dataset(dataset_name)

    def datasets(self):
        datasets = []
        for dataset_type in self.scraper.PATH.keys():
            datasets.extend(self.scraper.get_datasets(dataset_type))

        return list(sorted(datasets))

    def exists(self, dataset_name):
        return dataset_name in self.datasets()


class D3MManager(BaseManager):

    def __init__(self, username, password, skip_sublevels=False):
        self.scraper = D3MScraper(username, password, skip_sublevels)


class IPFSManager(BaseManager):

    def __init__(self, skip_sublevels=False):
        self.scraper = IPFSScraper(skip_sublevels)
