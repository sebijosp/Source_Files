CREATE OR REPLACE VIEW `hem`.`IPDR_S15_vw` as SELECT 
`cmts_host_name`,
`cmts_sys_up_time`,
`cmts_md_if_index`,
`ds_if_index`,
`ds_if_name`,
`ds_chid`,
`ds_util_interval`,
`ds_util_index_percentage`,
`ds_util_total_bytes`,
`ds_util_used_bytes`,
`rec_type`,
`cmts_ip_address`,
`hdp_insert_ts`,
`hdp_update_ts`,
`hdp_file_name`,
`job_execution_id`,
`processed_ts`
from `ipdr`.`IPDR_S15` 
WHERE to_date(processed_ts) = DATE_SUB(CURRENT_DATE(), 1);
