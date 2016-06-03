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

### Adding downloads

```bash
esgf_add_downloads.py -db db.sqlite -L debug -p CMIP5 -v taxmin -v tasmax -v pr -t day -x rcp26 -x rcp45 -x rcp60 -x rcp85 -x historical -x historicalMisc -x historicalGHG -x historicalExt -x historicalNat
```

### Fetching downloads

```bash
esgf_fetch_downloads.py -db db.sqlite -L debug -o <output_dir>
```
