CREATE TABLE cdrdm.`nsd_master_monthly_rollup_mstr`(
  `sitename` varchar(10),
  `monthly_ttl_rev` double,
  `cnt_detractors_nps` bigint,
  `cnt_passive_nps` bigint,
  `cnt_promoters_nps` bigint,
  `pct_detractors_nps` double,
  `pct_passive_nps` double,
  `pct_promoters_nps` double,
  `avg_nps` double,
  `nps_surveys` double,
  `monthly_data_volume` bigint,
  `monthly_avg_dl_thruput` double,
  `mothly_prcnt_sess_abv_5mbps` double,
  `monthly_lte_unavailability` double,
  `monthly_volte_ran_access` double,
  `monthly_volte_ran_drop_rate` double,
  `loc_code` varchar(10),
  `emg` varchar(6),
  `site_name` varchar(48),
  `province` varchar(3),
  `y_latitude` double,
  `x_longitude` double,
  `city` varchar(50),
  `market_size` varchar(10),
  `planning_area` varchar(50),
  `special_site_list` varchar(10),
  `port_out` bigint,
  `subscriber_cnt` bigint,
  `churn_score` double)
PARTITIONED BY (
  `month_ending` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
  'orc.compress'='ZLIB',
  'transient_lastDdlTime'='1533830743')
