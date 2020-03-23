------------------------------------------------------------
------------------------------------------------------------
-- 9. Datasource table/view and Datasource related tables/views
------------------------------------------------------------
------------------------------------------------------------
-- Datasource table creation & update
-------------------------------------
-- Creating and populating temporary datasource table
DROP TABLE IF EXISTS stats_wf_db_obs.datasource_tmp;
create table stats_wf_db_obs.datasource_tmp(`id` string, `name` string, `type` string, `dateofvalidation` string, `yearofvalidation` string, `harvested` boolean, `piwik_id` int, `latitude` string, `longitude` string, `websiteurl` string, `compatibility` string) clustered by (id) into 100 buckets stored as orc tblproperties('transactional'='true');
insert into stats_wf_db_obs.datasource_tmp SELECT substr(d.id, 4) as id, officialname.value as name, datasourcetype.classname as type, dateofvalidation.value as dateofvalidation, date_format(d.dateofvalidation.value,'yyyy') as yearofvalidation, false as harvested, 0 as piwik_id, d.latitude.value as latitude, d.longitude.value as longitude, d.websiteurl.value as websiteurl, d.openairecompatibility.classid as compatibility
from openaire.datasource d
WHERE d.datainfo.deletedbyinference=false;

-- Updating temporary table with everything that is not based on results -> This is done with the following "dual" table. To see if default values are there
-- Creating a temporary dual table that will be removed after the following insert
CREATE TABLE stats_wf_db_obs.dual(dummy char(1));
INSERT INTO stats_wf_db_obs.dual values('X');
INSERT INTO stats_wf_db_obs.datasource_tmp (`id`, `name`, `type`, `dateofvalidation`, `yearofvalidation`, `harvested`, `piwik_id`, `latitude`, `longitude`, `websiteurl`, `compatibility`) 
SELECT 'other', 'Other', 'Repository', NULL, NULL, false, 0, NULL, NULL, NULL, 'unknown' FROM stats_wf_db_obs.dual WHERE 'other' not in (SELECT id FROM stats_wf_db_obs.datasource_tmp WHERE name='Unknown Repository');
DROP TABLE stats_wf_db_obs.dual;

UPDATE stats_wf_db_obs.datasource_tmp SET name='Other' where name='Unknown Repository';
UPDATE stats_wf_db_obs.datasource_tmp SET yearofvalidation=null WHERE yearofvalidation='-1';

DROP TABLE IF EXISTS stats_wf_db_obs.datasource_languages;
CREATE TABLE stats_wf_db_obs.datasource_languages AS SELECT substr(d.id, 4) as id, langs.languages as language from openaire.datasource d LATERAL VIEW explode(d.odlanguages.value) langs as languages;
DROP TABLE IF EXISTS stats_wf_db_obs.datasource_oids;
CREATE TABLE stats_wf_db_obs.datasource_oids AS SELECT substr(d.id, 4) as id, oids.ids as oid from openaire.datasource d LATERAL VIEW explode(d.originalid) oids as ids;
DROP TABLE IF EXISTS stats_wf_db_obs.datasource_organizations;
CREATE TABLE stats_wf_db_obs.datasource_organizations AS select substr(r.target, 4) as id, substr(r.source, 4) as organization from openaire.relation r where r.reltype='datasourceOrganization';

CREATE OR REPLACE VIEW stats_wf_db_obs.datasource_results AS SELECT datasource AS id, id AS result FROM stats_wf_db_obs.result_datasources;
