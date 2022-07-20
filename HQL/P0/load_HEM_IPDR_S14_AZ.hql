!echo ;
!echo Hive : Drop Partitions;


Alter table hem.ipdr_s14_az drop if exists partition (processed_ts  > '1970-01-01');

!echo;
!echo Hive :Insert into the table;

Insert overwrite  table  hem.ipdr_s14_az Partition(processed_ts)
select 
cmts_host_name
,cmts_sys_up_time
,cmts_md_if_index
,us_if_index
,us_if_name
,us_ch_id
,us_util_interval
,us_util_index_percentage
,us_util_total_mslots
,us_util_ucastgranted_mslots
,us_util_total_cntn_mslots
,us_util_used_cntn_mslots
,us_util_coll_cntn_mslots
,us_util_total_cntn_req_mslots
,us_util_used_cntn_req_mslots
,us_util_coll_cntn_req_mslots
,us_util_total_cntn_req_data_mslots
,us_util_used_cntn_req_data_mslots
,us_util_coll_cntn_req_data_mslots
,us_util_total_cntn_init_maint_mslots
,us_util_used_cntn_init_maint_mslots
,us_util_coll_cntn_init_maint_mslots
,rec_type
,cmts_ip_address
,hdp_insert_ts
,hdp_update_ts
,hdp_file_name
,job_execution_id,current_timestamp as Az_insert_ts, current_timestamp as Az_update_ts, 0 as exec_run_id, processed_ts
from ipdr.ipdr_s14 as a 
where processed_ts >=  '${hiveconf:DeltaPartStart}' and processed_ts <  '${hiveconf:DeltaPartEnd}';


!echo ;
!echo Hive : Data loaded; 
