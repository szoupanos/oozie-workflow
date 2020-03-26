-- The following throws the following exception on CRN HUE Hive:
-- Error while compiling statement: FAILED: SemanticException [Error 10011]: Line 2:34 Invalid function 'date_format'

-- INSERT INTO ${stats_db_name}.software_tmp select substr(s.id, 4) as id, s.title[0].value as title, s.publisher.value as publisher, cast(null as string) as journal, 
-- s.dateofacceptance.value as date, date_format(s.dateofacceptance.value,'yyyy') as year, s.bestaccessright.classname as bestlicence,
-- s.embargoenddate.value as embargo_end_date, false as delayed, size(s.author) as authors , concat_ws(';',s.source.value) as source,
--  case when size(s.description) > 0 then true else false end as abstract,
-- 'software' as type
-- from ${openaire_db_name}.software s
-- where s.datainfo.deletedbyinference=false;
