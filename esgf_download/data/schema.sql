CREATE TABLE transfert (transfert_id INTEGER PRIMARY KEY, model TEXT, location TEXT,local_image TEXT, checksum TEXT, duration INT, fsize INT, rate INT, start_date TEXT,end_date TEXT, status TEXT, error_msg TEXT, crea_date TEXT, priority INT,variable TEXT,dimension_time INT,dimension_lat INT,dimension_lon INT,dimension_lev INT,tracking_id TEXT,version_xml_tag TEXT,size_xml_tag TEXT,checksum_type TEXT, local_product TEXT, product_xml_tag TEXT, dataset_id INT, discovery_engine INT);
CREATE INDEX idx_transfert_1 on transfert (location);
CREATE INDEX idx_transfert_2 on transfert (status);
CREATE INDEX idx_transfert_3 on transfert (model);
CREATE INDEX idx_transfert_4 on transfert (priority);
CREATE INDEX idx_transfert_5 on transfert (crea_date);
CREATE INDEX idx_transfert_6 on transfert (transfert_id);
CREATE INDEX idx_transfert_7 on transfert (local_image);
CREATE INDEX idx_transfert_8 on transfert (dataset_id);
CREATE TABLE model (name TEXT, datanode TEXT, institute TEXT, description TEXT, max_data_thread INT, metadata_download_status TEXT);
CREATE INDEX idx_model_1 on model (name);

