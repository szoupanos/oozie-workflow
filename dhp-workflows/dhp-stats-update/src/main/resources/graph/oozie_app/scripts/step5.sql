--------------------------------------------------------
--------------------------------------------------------
-- 5. Software table/view and Software related tables/views
--------------------------------------------------------
--------------------------------------------------------

-- Software temporary table supporting updates
DROP TABLE IF EXISTS stats_wf_db_obs.software_tmp;
CREATE TABLE stats_wf_db_obs.software_tmp (   id STRING,   title STRING,   publisher STRING,   journal STRING,   date STRING,   year STRING,   bestlicence STRING,   embargo_end_date STRING,   delayed BOOLEAN,   authors INT,   source STRING,   abstract BOOLEAN,   type STRING )  clustered by (id) into 100 buckets stored as orc tblproperties('transactional'='true');
INSERT INTO stats_wf_db_obs.software_tmp select substr(s.id, 4) as id, s.title[0].value as title, s.publisher.value as publisher, cast(null as string) as journal, 
s.dateofacceptance.value as date, date_format(s.dateofacceptance.value,'yyyy') as year, s.bestaccessright.classname as bestlicence,
s.embargoenddate.value as embargo_end_date, false as delayed, size(s.author) as authors , concat_ws(';',s.source.value) as source,
-- case when length(s.description[0].value) > 0 then TRUE else false end as abstract,
 case when size(s.description) > 0 then true else false end as abstract,
'software' as type
from openaire.software s
where s.datainfo.deletedbyinference=false;


-- Software_citations
Create table stats_wf_db_obs.software_citations as select substr(s.id, 4) as id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") as result from openaire.software s  lateral view explode(s.extrainfo) citations as citation where xpath_string(citation.value, "//citation/id[@type='openaire']/@value") !="";

CREATE TABLE stats_wf_db_obs.software_classifications AS SELECT substr(p.id, 4) as id, instancetype.classname as type from openaire.software p LATERAL VIEW explode(p.instance.instancetype) instances as instancetype;
CREATE TABLE stats_wf_db_obs.software_concepts AS SELECT substr(p.id, 4) as id, contexts.context.id as concept from openaire.software p LATERAL VIEW explode(p.context) contexts as context;

CREATE TABLE stats_wf_db_obs.software_datasources as SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource FROM (SELECT  substr(p.id, 4) as id, substr(instances.instance.hostedby.key, 4) as datasource 
from openaire.software p lateral view explode(p.instance) instances as instance) p LEFT OUTER JOIN
(SELECT substr(d.id, 4) id from openaire2.datasource d WHERE d.datainfo.deletedbyinference=false) d on p.datasource = d.id;

CREATE TABLE stats_wf_db_obs.software_languages AS select substr(p.id, 4) as id, p.language.classname as language from openaire.software p;
CREATE TABLE stats_wf_db_obs.software_oids AS SELECT substr(p.id, 4) as id, oids.ids as oid from openaire.software p LATERAL VIEW explode(p.originalid) oids as ids;
create table stats_wf_db_obs.software_pids as select substr(p.id, 4) as id, ppid.qualifier.classname as type, ppid.value as pid from openaire.software p lateral view explode(p.pid) pids as ppid;
create table stats_wf_db_obs.software_topics as select substr(p.id, 4) as id, subjects.subject.qualifier.classname as type, subjects.subject.value as topic from openaire.software p lateral view explode(p.subject) subjects as subject;
