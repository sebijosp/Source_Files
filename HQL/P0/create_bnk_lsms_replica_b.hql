create table cdrdm.bnk_lsms_replica_b(
tn varchar(20),
spid varchar(4),
sys_creation_date timestamp,
sys_update_date timestamp,
operator_id bigint,
application_id varchar(6),
dl_service_code varchar(5),
dl_update_stamp bigint
)
row format delimited
fields terminated by ",";
