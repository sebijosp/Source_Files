drop table cdrdm.nsd_data_validator;
create table cdrdm.nsd_data_validator(
tbl_type varchar(20),
update_freq varchar(10),
tbl_name varchar(50),
rec_cnt bigint 
)
partitioned by (load_dt string)
STORED AS ORC;
