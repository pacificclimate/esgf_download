Examples of :mod:`esgf_download` usage
======================================

It is generally intended that the ``esgf_add_downloads.py`` and ``esgf_fetch_downloads.py`` interfaces will be used. Thus, these examples will cover usage of that interface.

Retrieve information about all daily precipitation data from CCSM4 runs which are part of the CMIP5 project and follow the rcp45 emissions scenario.::

  esgf_add_downloads.py -db ccsm4_dl.sqlite3 -m CCSM4 -p CMIP5 -x rcp45 -t day -v pr

Since information is stored in an sqlite database, we can add data whenever we want. The example below illustrates adding daily temperature data to the selection::

  esgf_add_downloads.py -db ccsm4_dl.sqlite3 -m CCSM4 -p CMIP5 -x rcp45 -t day -v tas

We can also specify multiple variables, experiments, etc to add to the selections. The example below adds rcp26 and rcp60 data for both temperature and precipitation to this set::

  esgf_add_downloads.py -db ccsm4_dl.sqlite3 -m CCSM4 -p CMIP5 -x rcp26 -x rcp60 -t day -v tas -v pr

Finally, we can download the data. The example below downloads the default number of files at a time (5 per host, 50 overall)::
  
  esgf_fetch_downloads.py -db ccsm4.sqlite3 -o output_dir/ -u <username> -p <password>

At any point, you can hit control-C to stop downloading data. No download resuming is supported at this time; so only completed downloads will be kept (partials will be deleted).

Any download that doesn't match its checksum will be deleted and the status of that transfer will be set to 'error'.

No status reporting is supported at this time; however, it's not hard to query the sqlite3 database. Examples:

Open the database in sqlite3::

  sqlite3 downloads.sqlite3 
  SQLite version 3.7.13 2012-06-11 02:05:22
  Enter ".help" for instructions
  Enter SQL statements terminated with a ";"
  sqlite> 

Check on the status of your downloads::

  sqlite> SELECT status,COUNT(status) from transfert group by status;
  done|30948
  error|150

See what the breakdown is by variable::

  sqlite> SELECT variable,COUNT(variable) from transfert group by variable;
  pr|10384
  psl|10
  tas|12
  tasmax|10383
  tasmin|10309

Or by model::

  sqlite> SELECT model,COUNT(model) from transfert group by model;
  ACCESS1-0|45
  ACCESS1.3|42
  ...

Look for transfers with errors::

  sqlite> SELECT transfert_id, error_msg from transfert where status='error' LIMIT 50;
  44284|FILE_NOT_FOUND
  44293|FILE_NOT_FOUND
  44294|CHECKSUM_MISMATCH_ERROR
  44309|AUTH_FAIL
  44984|REQUESTS_UNKNOWN_ERROR: HTTPConnectionPool(host='esgdata.gfdl.noaa.gov', port=80): Max retries exceeded with url: /thredds/fileServer/gfdl_dataroot/NOAA-GFDL/GFDL-CM3/rcp45/day/atmos/day/r3i1p1/v20110601/pr/pr_day_GFDL-CM3_rcp45_r3i1p1_20910101-20951231.nc (Caused by <class 'socket.error'>: [Errno 111] Connection refused)

Look into a particular transfer::

  sqlite> SELECT * from transfert WHERE transfert_id = 44284;
  44284|EC-EARTH|http://esg2.e-inis.ie/thredds/fileServer/esg_dataroot/CMIP5/output/ICHEC/EC-EARTH/historical/day/atmos/pr/r11i1p1/pr_day_EC-EARTH_historical_r11i1p1_19000101-19241231.nc|CMIP5/output1/ICHEC/EC-EARTH/historical/day/atmos/day/r11i1p1/v20120202/pr/pr_day_EC-EARTH_historical_r11i1p1_19000101-19241231.nc|d85a20108d092b154c5756f96f9b5761|4.25319790840149||0|1374280334.31845|1374280338.57165|error|FILE_NOT_FOUND|||pr|||||a5815e91-a7bb-4605-8ae5-f99b77215830|v20120202|1870267804|MD5|output1|||

Finally, you can see the schema for the transfert table by issuing the following command::

  sqlite> .schema transfert
  CREATE TABLE transfert (transfert_id INTEGER PRIMARY KEY, model TEXT, location TEXT,local_image TEXT, checksum TEXT, duration INT, fsize INT, rate INT, start_date TEXT,end_date TEXT, status TEXT, error_msg TEXT, crea_date TEXT, priority INT,variable TEXT,dimension_time INT,dimension_lat INT,dimension_lon INT,dimension_lev INT,tracking_id TEXT,version_xml_tag TEXT,size_xml_tag TEXT,checksum_type TEXT, local_product TEXT, product_xml_tag TEXT, dataset_id INT, discovery_engine INT);

That's all. Hope this helps.
