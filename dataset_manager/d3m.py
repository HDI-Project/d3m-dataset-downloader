import json
import time

import requests
from bs4 import BeautifulSoup


class D3MScraper(object):

    auth_ldap_url = 'https://api-token.datadrivendiscovery.org/auth/ldap'
    auth_headers = {
        'Content-Type': 'application/json'
    }

    base_urls = {
        'seed': 'https://datadrivendiscovery.org/data/seed_datasets_current/',
        'll0': 'https://datadrivendiscovery.org/data/training_datasets/LL0/',
        'll1': 'https://datadrivendiscovery.org/data/training_datasets/LL1/'
    }

    dataset_types = dict()

    @classmethod
    def get_token(cls, username, password):
        data = {
            'username': username,
            'password': password
        }
        r = requests.post(cls.auth_ldap_url,
                          headers=cls.auth_headers,
                          data=json.dumps(data))

        try:
            return r.json()['access_token']

        except TypeError:
            raise Exception("Invalid username or password")

    def __init__(self, username, password, delay=0, skip_sublevels=False):
        self.session = requests.session()
        self.session.headers = {
            'Authorization': self.get_token(username, password)
        }
        self.delay = delay
        self.sublevels = ['tables/'] if skip_sublevels else []

    def get_url(self, url, raw=False):
        print("Getting URL {}".format(url))
        time.sleep(self.delay)
        r = self.session.get(url)
        if raw:
            return r.content

        else:
            return BeautifulSoup(r.text, 'html.parser')

    @staticmethod
    def get_links(soup):
        tds = soup.find_all('td', class_='display-name')
        links = (td.find('a') for td in tds)
        return [a.text for a in links if a.text != '../']

    def get_datasets(self, dataset_type):
        soup = self.get_url(self.base_urls[dataset_type])
        links = self.get_links(soup)
        return [link[:-1] for link in links if link[-1] == '/']

    def get_sublevel(self, base_path, level, sublevels):
        data = dict()

        if sublevels and level not in sublevels:
            return data

        level_url = base_path + level
        soup = self.get_url(level_url)

        links = self.get_links(soup)
        for link in links:
            if link[-1] == '/':
                # This is a link to a subfolder
                data[link[:-1]] = self.get_sublevel(level_url, link, sublevels)
            else:
                data[link] = self.get_url(level_url + link, raw=True)

        return data

    def get_base_url(self, dataset_name):
        if not self.dataset_types:
            for dataset_type, url in self.base_urls.items():
                datasets = self.get_datasets(dataset_type)
                for dataset in datasets:
                    self.dataset_types[dataset] = url

        return self.dataset_types[dataset_name]

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
            link[:-1]: self.get_sublevel(base_url, link, sublevels) for link in root_links
        }


class D3MManager(object):

    def __init__(self, username, password, skip_sublevels=False):
        self.scraper = D3MScraper(username, password, 0, skip_sublevels)

    def load(self, dataset_name, raw='to_be_ignored'):
        return self.scraper.scrape_dataset(dataset_name)

    def datasets(self):
        datasets = []
        for dataset_type in self.scraper.base_urls.keys():
            datasets.extend(self.scraper.get_datasets(dataset_type))

        return list(sorted(datasets))

    def exists(self, dataset_name):
        return dataset_name in self.datasets()
