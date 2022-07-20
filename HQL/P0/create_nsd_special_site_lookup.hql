drop table cdrdm.nsd_special_site_lookup;
create table cdrdm.nsd_special_site_lookup(
tower_id varchar(10),
special_site varchar(10))
row format delimited 
fields terminated by ',';
