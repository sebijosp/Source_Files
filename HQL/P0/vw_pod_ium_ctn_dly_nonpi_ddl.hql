create or replace view data_pods_nonpi.vw_pod_ium_ctn_dly as
select 
  if(BAN IS NOT NULL, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(BAN AS STRING))) AS VARCHAR(64)),NULL) as ban,
  if(subscriber_no IS NOT NULL, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(subscriber_no AS STRING))) AS VARCHAR(64)),NULL) as subscriber_no,
  brand, 
  segment_desc , 
  idv_ind , 
  evp_ind , 
  trans_type, 
  trans_dt, 
  notif_category ,
  session_id , 
  notification_id , 
  notif_scenario_id , 
  ium_75_ind,
  ium_90_ind , 
  ium_100_ind , 
  ium_suspend_ind ,
  calendar_year , 
  calendar_month  from data_pods.pod_ium_ctn_dly;

