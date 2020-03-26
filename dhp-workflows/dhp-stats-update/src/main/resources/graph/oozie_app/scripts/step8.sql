----------------------------------------------------
----------------------------------------------------
-- 8. Result table/view and Result related tables/views
----------------------------------------------------
----------------------------------------------------

-- Views on temporary tables that should be re-created in the end
CREATE OR REPLACE VIEW stats_wf_db_obs.result as SELECT *, bestlicence as access_mode FROM stats_wf_db_obs.publication_tmp UNION ALL SELECT *,bestlicence as access_mode FROM stats_wf_db_obs.software_tmp UNION ALL SELECT *,bestlicence as access_mode FROM stats_wf_db_obs.dataset_tmp UNION ALL SELECT *,bestlicence as access_mode FROM stats_wf_db_obs.otherresearchproduct_tmp;

-- Views on final tables
CREATE OR REPLACE VIEW stats_wf_db_obs.result_datasources as SELECT * FROM stats_wf_db_obs.publication_datasources UNION ALL SELECT * FROM stats_wf_db_obs.software_datasources UNION ALL SELECT * FROM stats_wf_db_obs.dataset_datasources UNION ALL SELECT * FROM stats_wf_db_obs.otherresearchproduct_datasources;

CREATE OR REPLACE VIEW stats_wf_db_obs.result_citations as SELECT * FROM stats_wf_db_obs.publication_citations UNION ALL SELECT * FROM stats_wf_db_obs.software_citations UNION ALL SELECT * FROM stats_wf_db_obs.dataset_citations UNION ALL SELECT * FROM stats_wf_db_obs.otherresearchproduct_citations;

CREATE OR REPLACE VIEW stats_wf_db_obs.result_classifications as SELECT * FROM stats_wf_db_obs.publication_classifications UNION ALL SELECT * FROM stats_wf_db_obs.software_classifications UNION ALL SELECT * FROM stats_wf_db_obs.dataset_classifications UNION ALL SELECT * FROM stats_wf_db_obs.otherresearchproduct_classifications;

CREATE OR REPLACE VIEW stats_wf_db_obs.result_concepts as SELECT * FROM stats_wf_db_obs.publication_concepts UNION ALL SELECT * FROM stats_wf_db_obs.software_concepts UNION ALL SELECT * FROM stats_wf_db_obs.dataset_concepts UNION ALL SELECT * FROM stats_wf_db_obs.otherresearchproduct_concepts;

CREATE OR REPLACE VIEW stats_wf_db_obs.result_languages as SELECT * FROM stats_wf_db_obs.publication_languages UNION ALL SELECT * FROM stats_wf_db_obs.software_languages UNION ALL SELECT * FROM stats_wf_db_obs.dataset_languages UNION ALL SELECT * FROM stats_wf_db_obs.otherresearchproduct_languages;

CREATE OR REPLACE VIEW stats_wf_db_obs.result_oids as SELECT * FROM stats_wf_db_obs.publication_oids UNION ALL SELECT * FROM stats_wf_db_obs.software_oids UNION ALL SELECT * FROM stats_wf_db_obs.dataset_oids UNION ALL SELECT * FROM stats_wf_db_obs.otherresearchproduct_oids;

CREATE OR REPLACE VIEW stats_wf_db_obs.result_pids as SELECT * FROM stats_wf_db_obs.publication_pids UNION ALL SELECT * FROM stats_wf_db_obs.software_pids UNION ALL SELECT * FROM stats_wf_db_obs.dataset_pids UNION ALL SELECT * FROM stats_wf_db_obs.otherresearchproduct_pids;

CREATE OR REPLACE VIEW stats_wf_db_obs.result_topics as SELECT * FROM stats_wf_db_obs.publication_topics UNION ALL SELECT * FROM stats_wf_db_obs.software_topics UNION ALL SELECT * FROM stats_wf_db_obs.dataset_topics UNION ALL SELECT * FROM stats_wf_db_obs.otherresearchproduct_topics;

DROP TABLE IF EXISTS stats_wf_db_obs.result_organization;
CREATE TABLE stats_wf_db_obs.result_organization AS SELECT substr(r.target, 4) as id, substr(r.source, 4) as organization from openaire.relation r where r.reltype='resultOrganization';

DROP TABLE IF EXISTS stats_wf_db_obs.result_projects;
CREATE TABLE stats_wf_db_obs.result_projects AS select pr.result as id, pr.id as project, datediff(p.enddate, p.startdate) as daysfromend from stats_wf_db_obs.result r join stats_wf_db_obs.project_results pr on r.id=pr.result join stats_wf_db_obs.project_tmp p on p.id=pr.id;
