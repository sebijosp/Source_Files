CREATE EXTERNAL TABLE `IPTV`.`Ignite_Pods_daily_raw`(
  `generated_date` bigint,
  `partner_name` string,
  `cpe_mac` string,
  `pod_mac` string,
  `pod_id` string,
  `pod_pack_id` string,
  `pod_state` string,
  `pod_intf_type` string,
  `pod_rssi` string,
  `pod_score` string,
  `pod_health_status` string,
  `pod_nickname` string,
  `pod_first_seen_date` string,
  `pod_firmware` string,
  `pod_ip_address` string,
  `pod_model_number` string,
  `pod_client_devices_count` string,
  `pod_leaf_to_root_nickname` string,
  `pod_leaf_to_root_id` string,
  `pod_model_name` string,
hdp_create_ts       STRING,
    hdp_update_ts       STRING
)
PARTITIONED BY (snapshot_date string)
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true")
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/apps/hive/warehouse/iptv.db/Ignite_Pods_daily_raw/';
