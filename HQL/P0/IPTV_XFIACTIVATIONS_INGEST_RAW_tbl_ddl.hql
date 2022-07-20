CREATE EXTERNAL TABLE iptv.xfiActivationsIngest_raw(
  `accountId` string,
  `deviceId`  string,
  `x1DeviceId`  string,
  `deviceInfo` string,
  `deviceType` string,
  `activationDuration` bigint,
  `errorCode`   string,
  `errorDescription`    string,
  `hdp_update_ts`  timestamp,
  `hdp_create_ts`  timestamp,
  `hdp_file_name`  string)
PARTITIONED BY (
  `received_date` date)
ROW FORMAT SERDE
  'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json'='true',
  'quoteChar'= '\\"')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/apps/hive/warehouse/iptv.db/xfiActivationsIngest_raw';
