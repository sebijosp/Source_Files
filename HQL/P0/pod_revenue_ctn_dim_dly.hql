drop table data_pods.pod_revenue_ctn_dim_dly;
CREATE TABLE data_pods.pod_revenue_ctn_dim_dly(
  ecid bigint,
  ban bigint, 
  subscriber_no bigint, 
  base double, 
  base_data double, 
  data_browsing double, 
  data_other double, 
  data_overage double, 
  data_roam double, 
  device double, 
  ecp double, 
  fees double, 
  ld_can double, 
  ld_us_intl double, 
  other double, 
  sms double, 
  sms_us_intl double, 
  voice_overage double, 
  voice_roam double, 
  wes double, 
  hpg double, 
  MDT double,
  ONE_TIME_DTU double,
  MM_RPU double,
  rpu double,
  hdp_update_ts timestamp)
PARTITIONED BY (
  bill_year int,
  bill_month int)
tblproperties ("orc.compress"="SNAPPY");
