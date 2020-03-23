----------------------------------------------------------------
----------------------------------------------------------------
-- Organization table/view and Organization related tables/views
----------------------------------------------------------------
----------------------------------------------------------------
DROP TABLE IF EXISTS stats_wf_db_obs.organization;
CREATE TABLE stats_wf_db_obs.organization AS SELECT substr(o.id, 4) as id, o.legalname.value as name, o.country.classid as country from openaire.organization o WHERE o.datainfo.deletedbyinference=false;

CREATE OR REPLACE VIEW stats_wf_db_obs.organization_datasources AS SELECT organization AS id, id AS datasource FROM stats_wf_db_obs.datasource_organizations;
CREATE OR REPLACE VIEW stats_wf_db_obs.organization_projects AS SELECT id AS project, organization as id FROM stats_wf_db_obs.project_organizations;
