from setuptools import setup, find_packages

__version__ = (0, 0, 1)

setup(
    name='esgf_download',
    description='Utility to download data from ESGF federation servers',
    keywords='sql database data science climate',
    #package_dir = { '': 'src' },
    package_dir = { 'esgf_download': 'src/esgf_download' },
    version='.'.join(str(d) for d in __version__),
    url='http://www.pacificclimate.org/resources/software-library',
    author='David Bronaugh for the Pacific Climate Impacts Consortium',
    author_email='bronaugh@uvic.ca',
    packages=find_packages('src'),
    #packages=[ 'esgf_download' ],
    scripts = [ 'scripts/esgf_add_downloads.py', 'scripts/esgf_fetch_downloads.py' ],
    package_data = { 'esgf_download': [ 'data/schema.sql' ] },
    install_requires = [ 'requests',
                         'esgf-pyclient',
                         'MyProxyClient', #Actually required by esgf-pyclient
                         'lxml' ],
    include_package_data=True,
    license='GPL-2.1',
    
)
