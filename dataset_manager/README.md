# Programmatic D3M Dataset Manager

The dataset manager allows easy management of the D3M datasets.

It supports:

* downloading datasets from D3M repository or from S3 buckets
* uploading datasets to S3 buckets
* loading or saving datasets to local filesystem
* spliting datasets into TRAIN, TEST and SCORE subsets following the dataSplits.csv indexes

## Installation

```
pip install -r requirements.txt
```

### D3M Repository

In order to interact with the D3M repository you will need the user and the password
user to log into https://datadrivendiscovery.org/data

### S3 Bucket

In order to interact with the S3 buckets, you will need to configure your S3 access
following the instructions from http://boto3.readthedocs.io/en/latest/guide/quickstart.html

In most cases, it will be enough to create the file `~/.aws/credentials:`
with the following contents:

```
[default]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
```

## Usage

The main script is the `download_manager.py`, which supports the following options:

- **-i, --input** - D3M website, S3 bucket or local folder.
- **-o, --output** - S3 bucket or local folder.
- **-l, --list** - List all available datasets in the indicated input.
- **-a, --all** - Get and process all available datasets in the indicated input.
- **-s, --split** - Split the dataset using the dataSplits.csv indexes.
- **-r, --raw** - Do not download the splitted subsets. `-s` option implicitly enables this one.
- **-f, --force** - Overwrite any existing datasets. If not enabled, existing datasets will be skipped.
- **dataset names** - Name of the datasets o download. The `-a` option overrides them.


## Input and Output

The Input and Output options implicitely point at different locations depending on the format:

* **D3M**: `d3m:username:passsword`: password can be omitted, as well as username. Accepted only as Input.
If omitted, the user will be asked to insert them later on.
* **S3**: `s3:bucket-name`: The datasets will be stored in the root of the bucket.
* **Local filesystem**: `local/filesystem/path`: The path must exist, otherwise it raises an error.


## Usage Example

Download all datasets from D3M and store them as they are into S3 bucket named `d3m-data-dai`.
This will skip existing datasets.

```
python dataset_manager.py -i d3m:a_username:a_password -o s3:d3m-data-dai -a
```

Download all datasets from D3M, split them and store them in a local folder `datasets`, overwriting
any existing data.
This will prompt the user for the d3m password.

```
python dataset_manager.py -i d3m:a_username -o datasets -a -s -f
```

Download the datasets `185_baseball` and `32_wikiqa` from S3 bucket `d3m-data-dai`
into local folder `data/datasets`. Overwrite the existing data.

```
python dataset_manager.py -i s3:d3m-data-dai -o data/datasets -f 185_baseball 32_wikiqa
```
