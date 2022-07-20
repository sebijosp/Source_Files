!echo ;
!echo Hive : Drop Partitions;


Alter table hem.ipdr_s15_az drop if exists partition (processed_ts  > '1970-01-01');

!echo;
!echo Hive :Insert into the table;

Insert overwrite  table  hem.ipdr_s15_az Partition(processed_ts)
select 
cmts_host_name
,cmts_sys_up_time
,cmts_md_if_index
,ds_if_index
,ds_if_name
,ds_chid
,ds_util_interval
,ds_util_index_percentage
,ds_util_total_bytes
,ds_util_used_bytes
,rec_type
,cmts_ip_address
,hdp_insert_ts
,hdp_update_ts
,hdp_file_name
,job_execution_id
,current_timestamp as Az_insert_ts, current_timestamp as Az_update_ts, 0 as exec_run_id, processed_ts
from ipdr.ipdr_s15 as a 
where processed_ts >=  '${hiveconf:DeltaPartStart}' and processed_ts <  '${hiveconf:DeltaPartEnd}';


!echo ;
!echo Hive : Data loaded; 
