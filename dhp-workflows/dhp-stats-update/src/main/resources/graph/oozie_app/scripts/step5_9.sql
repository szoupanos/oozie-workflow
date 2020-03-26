create table ${stats_db_name}.software_pids as select substr(p.id, 4) as id, ppid.qualifier.classname as type, ppid.value as pid from ${openaire_db_name}.software p lateral view explode(p.pid) pids as ppid;