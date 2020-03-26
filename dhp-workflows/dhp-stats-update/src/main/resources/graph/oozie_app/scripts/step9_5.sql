-- Oozie error
-- Error: Error while compiling statement: FAILED: SemanticException [Error 10294]: Attempt to do update or delete using transaction manager that does not support these operations. (state=42000,code=10294)

-- UPDATE ${stats_db_name}.datasource_tmp SET yearofvalidation=null WHERE yearofvalidation='-1';
