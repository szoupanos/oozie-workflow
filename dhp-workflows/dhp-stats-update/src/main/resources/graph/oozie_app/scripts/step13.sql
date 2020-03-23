------------------------------------------------------------------------------------------------------
-- Creating parquet tables from the updated temporary tables and removing unnecessary temporary tables
------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS stats_wf_db_obs.datasource;
CREATE TABLE stats_wf_db_obs.datasource stored as parquet as select * from stats_wf_db_obs.datasource_tmp;

DROP TABLE IF EXISTS  stats_wf_db_obs.publication;
CREATE TABLE stats_wf_db_obs.publication stored as parquet as select * from stats_wf_db_obs.publication_tmp;

DROP TABLE IF EXISTS stats_wf_db_obs.dataset;
CREATE TABLE stats_wf_db_obs.dataset stored as parquet as select * from stats_wf_db_obs.dataset_tmp;

DROP TABLE IF EXISTS stats_wf_db_obs.software;
CREATE TABLE stats_wf_db_obs.software stored as parquet as select * from stats_wf_db_obs.software_tmp;

DROP TABLE IF EXISTS stats_wf_db_obs.otherresearchproduct;
CREATE TABLE stats_wf_db_obs.otherresearchproduct stored as parquet as select * from stats_wf_db_obs.otherresearchproduct_tmp;

DROP TABLE stats_wf_db_obs.project_tmp;
DROP TABLE stats_wf_db_obs.datasource_tmp;
DROP TABLE stats_wf_db_obs.publication_tmp;
DROP TABLE stats_wf_db_obs.dataset_tmp;
DROP TABLE stats_wf_db_obs.software_tmp;
DROP TABLE stats_wf_db_obs.otherresearchproduct_tmp;

