CREATE OR REPLACE VIEW `hem`.`ipdr_s10_vw` AS 
select 
`ipdr_s10`.`cmts_host_name`
,`ipdr_s10`.`cmts_sys_up_time`
,`ipdr_s10`.`cmts_md_if_name` 
,`ipdr_s10`.`cmts_md_if_index`
,`ipdr_s10`.`cm_mac_addr`
,`ipdr_s10`.`cm_reg_status_id`
,`ipdr_s10`.`cmts_cm_us_ch_if_name`
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
,`ipdr_s10`.`cmts_ip_address`
,`ipdr_s10`.`hdp_insert_ts`
,`ipdr_s10`.`hdp_update_ts`
,`ipdr_s10`.`hdp_file_name`
,`ipdr_s10`.`job_execution_id`
,`ipdr_s10`.`processed_ts` 
FROM `ipdr`.`ipdr_s10` where to_date(`ipdr_s10`.`processed_ts`) = DATE_SUB(CURRENT_DATE(), 1);
