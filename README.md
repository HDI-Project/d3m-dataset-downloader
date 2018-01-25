# Programmatic D3M Dataset Downloader

## Installation

```
pip install -r requirements.txt
```

## Usage

First you'll need to get an API token from here:
https://api-token.datadrivendiscovery.org

Then, set that token as an environment variable:
```
export TOKEN={YOUR_API_TOKEN}
```

#### Save all the dataset docs from a particular cohort as follows:

```
from d3m_downloader import save_dataset_docs
save_dataset_docs("seed", 'seed_dataset_docs.json')

save_dataset_docs("ll0", 'll0_training_datasets_url.json')

save_dataset_docs("ll1", 'll1_training_datasets_url.json')
```

#### Download a dataset of a particular name to a specified folder:

```
from d3m_downloader import download_dataset_from_name
download_dataset_from_name('LL0_1008_analcatdata_reviewer', 'local_LL0_1008_dataset_folder')
```

#### Download all dataset of a particular type to a folder:
Arguments:
- **types_doc** - saved json with all the dataset docs from a particular source,
saved using `save_dataset_docs()`
- **ds_cohort** - one of "seed", "ll0", "ll1"
- **dtype** - type of dataset you want to save ("tabular", "image", "audio", etc.)
- **output_dir** - output directory to save to. You can specify an existing directory to add more datasets to it
- **max_datasets** - only download this many datasets

```
from d3m_downloader import download_dataset_from_type
download_dataset_from_type(types_doc='seed_dataset_docs.json',
                           ds_cohort='seed',
                           dtype='image',
                           output_dir='image_datasets',
                           max_datasets=2)
```
