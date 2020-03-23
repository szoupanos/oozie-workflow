------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
-- Tables/views from external tables/views (Fundref, Country, CountyGDP, roarmap, rndexpediture)
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW stats_wf_db_obs.fundref AS SELECT * FROM stats_ext.fundref;
CREATE OR REPLACE VIEW stats_wf_db_obs.country AS SELECT * FROM stats_ext.country;
CREATE OR REPLACE VIEW stats_wf_db_obs.countrygdp AS SELECT * FROM stats_ext.countrygdp;
CREATE OR REPLACE VIEW stats_wf_db_obs.roarmap AS SELECT * FROM stats_ext.roarmap;
CREATE OR REPLACE VIEW stats_wf_db_obs.rndexpediture AS SELECT * FROM stats_ext.rndexpediture;
