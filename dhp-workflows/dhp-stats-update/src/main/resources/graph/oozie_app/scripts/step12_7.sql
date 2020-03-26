-- Error: Error while compiling statement: FAILED: SemanticException [Error 10294]: Attempt to do update or delete using transaction manager that does not support these operations. (state=42000,code=10294)

-- Oherresearchproduct temporary table updates
-- UPDATE ${stats_db_name}.otherresearchproduct_tmp SET delayed = 'yes' WHERE otherresearchproduct_tmp.id IN (SELECT distinct r.id FROM ${stats_db_name}.result r, ${stats_db_name}.project_results pr, ${stats_db_name}.project_tmp p WHERE r.id=pr.result AND pr.id=p.id AND to_date(r.date)-to_date(p.enddate) > 0);
