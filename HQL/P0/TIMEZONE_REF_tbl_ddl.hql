CREATE TABLE `iptv.TIMEZONE_REF`(
  `city` string,
  `province` string,
  `country` string,
  `zip_fsa` string,
  `timezone_name` string,
  `timezone_type` string,
  `time_offset` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
