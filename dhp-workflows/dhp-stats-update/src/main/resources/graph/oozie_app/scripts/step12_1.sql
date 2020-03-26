----------------------------
-- Post processing - Updates
----------------------------

-- Oozie error
-- Error: Error while compiling statement: FAILED: SemanticException [Error 10294]: Attempt to do update or delete using transaction manager that does not support these operations. (state=42000,code=10294)

--Datasource temporary table updates
-- UPDATE ${stats_db_name}.datasource_tmp set harvested ='true' WHERE datasource_tmp.id IN (SELECT DISTINCT d.id FROM ${stats_db_name}.datasource_tmp d, ${stats_db_name}.result_datasources rd where d.id=rd.datasource);
