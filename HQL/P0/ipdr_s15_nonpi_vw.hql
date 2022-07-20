CREATE OR REPLACE VIEW `hem_nonpi`.`vw_IPDR_S15` as SELECT 
if(cmts_host_name is not null, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(cmts_host_name as string))) as varchar(64)), null) as cmts_host_name,
`cmts_sys_up_time`,
if(cmts_md_if_index is not null, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(cmts_md_if_index as string))) as varchar(64)), null) as cmts_md_if_index,
`ds_if_index`,
if(ds_if_name is not null, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(ds_if_name as string))) as varchar(64)), null) as ds_if_name,
`ds_chid`,
`ds_util_interval`,
`ds_util_index_percentage`,
`ds_util_total_bytes`,
`ds_util_used_bytes`,
`rec_type`,
if(cmts_ip_address is not null, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(cmts_ip_address as string))) as varchar(64)), null) as cmts_ip_address,
`hdp_insert_ts`,
`hdp_update_ts`,
if(hdp_file_name is not null, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(hdp_file_name as string))) as varchar(64)), null) as hdp_file_name,
`job_execution_id`,
`processed_ts`
from `hem`.`IPDR_S15`; 

