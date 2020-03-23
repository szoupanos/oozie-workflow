----------------------------------------------
-- Re-creating views from final parquet tables
---------------------------------------------

-- Result
CREATE OR REPLACE VIEW stats_wf_db_obs.result as SELECT *, bestlicence as access_mode FROM stats_wf_db_obs.publication UNION ALL SELECT *, bestlicence as access_mode FROM stats_wf_db_obs.software UNION ALL SELECT *, bestlicence as access_mode FROM stats_wf_db_obs.dataset UNION ALL SELECT *, bestlicence as access_mode FROM stats_wf_db_obs.otherresearchproduct;

