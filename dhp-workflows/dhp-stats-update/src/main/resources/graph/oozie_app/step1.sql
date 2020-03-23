DROP database if EXISTS stats_wf_db_spyros cascade;
CREATE database stats_wf_db_spyros;

CREATE TABLE stats_wf_db_spyros.Persons ( 
PersonID int, 
LastName varchar(255));

INSERT INTO stats_wf_db_spyros.Persons VALUES (1, "test_db_spyros_rec_1"); 
