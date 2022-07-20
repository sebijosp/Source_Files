!echo ;
!echo Hive : Drop Partitions;


Alter table hem.ipdr_s10_az drop if exists partition (processed_ts  > '1970-01-01');

!echo;
!echo Hive :Insert into the table;

Insert overwrite  table  hem.ipdr_s10_az Partition(processed_ts)
select cmts_host_name,	cmts_sys_up_time,	cmts_md_if_name,	cmts_md_if_index,	cm_mac_addr,	cm_reg_status_id,	cmts_cm_us_ch_if_name,	cmts_cm_us_ch_if_index,	cmts_cm_us_ch_id,	cmts_cm_us_modulation_type,	cmts_cm_us_rx_power,	cmts_cm_us_signal_noise,	cmts_cm_us_microreflections,	cmts_cm_us_eq_data,	cmts_cm_us_unerroreds,	cmts_cm_us_correcteds,	cmts_cm_us_uncorrectables,	cmts_cm_us_high_resolution_timing_offset,	cmts_cm_us_is_muted,	cmts_cm_us_ranging_status,	rec_type,	cmts_ip_address,	hdp_insert_ts,	hdp_update_ts,	hdp_file_name,	job_execution_id,
current_timestamp as Az_insert_ts, current_timestamp as Az_update_ts, 0 as exec_run_id, processed_ts
from ipdr.ipdr_s10 as a 
where processed_ts >=  '${hiveconf:DeltaPartStart}' and processed_ts <  '${hiveconf:DeltaPartEnd}';


!echo ;
!echo Hive : Data loaded; 
