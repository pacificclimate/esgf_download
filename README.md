# esgf_download

Earth Systems Grid Federation data downloader

## Installation

```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
python setup.py install
```
## Setup

### User setup

Firstly, downloading ESGF data requires a user account on one of the ESGF nodes. [LLNL](https://pcmdi.llnl.gov/projects/esgf-llnl/) works great. After creating an account, register for a "CMIP5 Research" role.

### Authentication

The easy method is to bootstrap the process and download the certs insecurely when authenticating:

```bash
myproxyclient logon -s <esgf_node> -p 7512 -l <esgf_username> -o /home/<username>/.esg/credentials.pem -T -b
```

The better way would be to get the CA certs:

```bash
wget --no-check-certificate https://rainbow.llnl.gov/dist/certs/esg_trusted_certificates.tar
wget https://rainbow.llnl.gov/dist/certs/esg_trusted_certificates.md5
md5sum -c esg_trusted_certificates.md5
tar -xf esg_trusted_certificates.tar
mkdir -p ~/.esg/certificates/
cp esg_trusted_certificates/* ~/.esg/certificates/
```

With the certs, you should now be able to obtain your credentials securely:

```bash
myproxyclient logon -s <esgf_node> -p 7512 -l <esgf_username> -o /home/<username>/.esg/credentials.pem
```

With the credentials in place, if you run the following, it should print 'True':

```python
python -c '''from pyesgf.logon import LogonManager
lm = LogonManager()
print lm.is_logged_on()'''
```

### Database setup

Create a new sqlite database and import the included schema:

```bash
cat esgf_download/data/schema.sql | sqlite3 db.sqlite
```

## Usage

The included scripts have built-in help accessed with the `-h` option.

### Adding downloads

```bash
esgf_add_downloads.py -db db.sqlite -L debug -p CMIP5 -v tasmin -v tasmax -v pr -t day -x rcp26 -x rcp45 -x rcp60 -x rcp85 -x historical -x historicalMisc -x historicalGHG -x historicalExt -x historicalNat
```

Search terms are passed directly as contraints to [pyesgf.search.SearchContext](http://esgf-pyclient.readthedocs.io/en/latest/search_api.html#module-pyesgf.search.context)

### Fetching downloads

```bash
esgf_fetch_downloads.py -db db.sqlite -L debug -o <output_dir> -u <username> -p <password> -a <auth_node>
```

### Aggregating the downloads

Downloaded files are typically split across time with each file consisting of a temporal subset. For local storage it is ideal to concatanate them together for one file per model run. Use the `aggregate_and_rename.r` script accomplish this.

The file aggregation system takes the downloaded tree and aggregates files as necessary to produce a tree containing single files which include all of the data available for a particular variable-model-emissions-run-version combination. This code is to be run within R. The functions of interest are:

* `get.file.metadata.cmip5`: Retrieves metadata for a filesystem tree, for use with the other functions mentioned.
* `aggregate.cmip5`: Aggregates (as needed) files described within the retrieved metadata, producing single files containing all of the data for a combination as defined above.
* `create.cmip5.symlink.tree`: Creates a symlink tree linking only to the aggregate files.

The sequence is typically:

* Get file metadata.
* Aggregate data.
* Get file metadata on new tree.
* Create symlink tree.

```R
> meta <- get.file.metadata.cmip5('/datasets/climate-CMIP5/nobackup/CMIP5/output1')
> agg_res <- aggregate.cmip5(meta)
> meta_after_agg <- get.file.metadata.cmip5('/datasets/climate-CMIP5/nobackup/CMIP5/output1/')
> create.cmip5.symlink.tree(meta_after_agg, '/home/data/projects/rat/test_cmip5_data')
```

If errors happen midway through aggregation, any partially created files must be cleaned up, `get.file.metadata` ran again, and the aggregation done using the new metadata result.
