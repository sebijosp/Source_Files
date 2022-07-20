drop table data_pods.pod_revenue_ban_dim_dly;
CREATE TABLE data_pods.pod_revenue_ban_dim_dly(
  ecid bigint,
  ban bigint,
  ban_base double,
  ban_base_data double,
  ban_data_browsing double,
  ban_data_other double,
  ban_data_overage double,
  ban_data_roam double,
  ban_device double,
  ban_ecp double,
  ban_fees double,
  ban_ld_can double,
  ban_ld_us_intl double,
  ban_other double,
  ban_sms double,
  ban_sms_us_intl double,
  ban_voice_overage double,
  ban_voice_roam double,
  ban_wes double,
  ban_hpg double,
  ban_MDT double,
  ban_ONE_TIME_DTU double,
  ban_MM_RPU double,
  ban_rpu double,
  hdp_update_ts timestamp)
PARTITIONED BY (
  bill_year int,
  bill_month int)
tblproperties ("orc.compress"="SNAPPY");
