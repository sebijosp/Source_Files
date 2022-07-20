set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table if not exists cdrdm.backup_gmpc_202202 like cdrdm.gmpc;
insert overwrite table cdrdm.backup_gmpc_202202 partition (file_date) select * from cdrdm.gmpc;

create table if not exists cdrdm.backup_fact_gmpc_cdr_202202 like cdrdm.fact_gmpc_cdr;
insert overwrite table cdrdm.backup_fact_gmpc_cdr_202202 partition (position_date) select * from cdrdm.fact_gmpc_cdr;
