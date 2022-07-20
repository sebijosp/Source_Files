CREATE OR REPLACE VIEW `hem_nonpi.vw_ipdr_s10`
as select 
if(cmts_host_name is not null, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(cmts_host_name as string))) as varchar(64)), null) as cmts_host_name
,`ipdr_s10`.`cmts_sys_up_time`
,if(cmts_md_if_name is not null, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(cmts_md_if_name as string))) as varchar(64)), null) as cmts_md_if_name 
,`ipdr_s10`.`cmts_md_if_index`
,if(cm_mac_addr is not null, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(cm_mac_addr as string))) as varchar(64)), null) as cm_mac_addr
,`ipdr_s10`.`cm_reg_status_id`
,if(cmts_cm_us_ch_if_name is not null, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(cmts_cm_us_ch_if_name as string))) as varchar(64)), null) as cmts_cm_us_ch_if_name
,`ipdr_s10`.`cmts_cm_us_ch_if_index`
,`ipdr_s10`.`cmts_cm_us_ch_id`
,`ipdr_s10`.`cmts_cm_us_modulation_type`
,`ipdr_s10`.`cmts_cm_us_rx_power`
,`ipdr_s10`.`cmts_cm_us_signal_noise`
,`ipdr_s10`.`cmts_cm_us_microreflections`
,`ipdr_s10`.`cmts_cm_us_eq_data`
,`ipdr_s10`.`cmts_cm_us_unerroreds`
,`ipdr_s10`.`cmts_cm_us_correcteds`
,`ipdr_s10`.`cmts_cm_us_uncorrectables`
,`ipdr_s10`.`cmts_cm_us_high_resolution_timing_offset`
,`ipdr_s10`.`cmts_cm_us_is_muted`
,`ipdr_s10`.`cmts_cm_us_ranging_status`
,`ipdr_s10`.`rec_type`
,if(cmts_ip_address is not null, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(cmts_ip_address as string))) as varchar(64)), null) as cmts_ip_address
,`ipdr_s10`.`hdp_insert_ts`
,`ipdr_s10`.`hdp_update_ts`
,if(hdp_file_name is not null, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(hdp_file_name as string))) as varchar(64)), null) as hdp_file_name
,`ipdr_s10`.`job_execution_id`
,`ipdr_s10`.`processed_ts` 
FROM `hem`.`ipdr_s10`;
