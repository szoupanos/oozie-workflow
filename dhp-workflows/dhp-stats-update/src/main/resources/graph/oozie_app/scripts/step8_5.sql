CREATE OR REPLACE VIEW ${stats_db_name}.result_concepts as SELECT * FROM ${stats_db_name}.publication_concepts UNION ALL SELECT * FROM ${stats_db_name}.software_concepts UNION ALL SELECT * FROM ${stats_db_name}.dataset_concepts UNION ALL SELECT * FROM ${stats_db_name}.otherresearchproduct_concepts;