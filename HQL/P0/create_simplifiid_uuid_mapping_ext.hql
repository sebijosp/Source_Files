CREATE EXTERNAL TABLE `simplifi.simplifiid_uuid_mapping_ext`(
  `Simplifi_id` string,
  `UUID` string
)row format delimited fields terminated by '\t'
LOCATION '${hiveconf:filePath}';
