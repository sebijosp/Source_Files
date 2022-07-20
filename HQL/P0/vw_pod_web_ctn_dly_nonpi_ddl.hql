create or replace view data_pods_nonpi.vw_pod_web_ctn_dly as
select
ecid,
if(BAN IS NOT NULL, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(BAN AS STRING))) AS VARCHAR(64)),NULL) as ban,
if(hashed_account_number IS NOT NULL, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(hashed_account_number AS STRING))) AS VARCHAR(64)),NULL) as hashed_account_number,
brand,  
session_date,
session_id,
session_date_time,
webpage,   
webpage_tr,
dynamic_content,
join_key,
calendar_year,
calendar_month from data_pods.pod_web_ban_dly;
