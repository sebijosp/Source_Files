!echo ;
!echo Hive : Drop Partitions;


Alter table hem.ipdr_s22_az drop if exists partition (processed_ts  > '1970-01-01');

!echo;
!echo Hive :Insert into the table;

Insert overwrite  table  hem.ipdr_s22_az Partition(processed_ts)
select cmts_host_name
,cm_mac_addr
,cmts_ds_if_index
,profile_id
,partial_chan_reason_code
,last_partial_chan_time
,last_partial_chan_reason_code
,rec_type
,rec_creation_time
,cmts_ip_address
,hdp_insert_ts
,hdp_update_ts
,hdp_file_name
,job_execution_id
,current_timestamp as Az_insert_ts, current_timestamp as Az_update_ts, 0 as exec_run_id, processed_ts
from ipdr.ipdr_s22 as a 
where processed_ts >=  '${hiveconf:DeltaPartStart}' and processed_ts <  '${hiveconf:DeltaPartEnd}';


!echo ;
!echo Hive : Data loaded; 
