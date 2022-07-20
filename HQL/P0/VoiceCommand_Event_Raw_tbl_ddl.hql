CREATE EXTERNAL TABLE `iptv`.`VoiceCommand_Event_Raw`(
`header` struct<`timestamp`:bigint,uuid:string,hostname:string,money:struct<traceId:string,spanId:bigint,parentSpanId:bigint,spanName:string,appName:string,startTime:bigint,spanDuration:bigint,spanSuccess:boolean,notes:map<string,string>>>,
`device` struct<receiverId:string,deviceId:string,deviceSourceId:string,account:string,accountSourceId:string,billingAccountId:string,macAddress:string,ecmMacAddress:string,firmwareVersion:string,deviceType:string,make:string,model:string,partner:string,ipAddress:string,utcOffset:int,postalCode:string,dma:string>,
eventTimestamp bigint,
status string,
statusCode string,
trx string,
transcription string,
codec string,
errorResponse string,
errorMessage string,
language string,
rmtType string,
executeAgent string,
nlpSource string,
nlpAction string,
domain string,
`hdp_file_name` string COMMENT 'from deserializer',
`hdp_create_ts` string COMMENT 'from deserializer',
`hdp_update_ts` string COMMENT 'from deserializer')
PARTITIONED BY (
  `received_date` date)
ROW FORMAT SERDE
  'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json'='true')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/apps/hive/warehouse/iptv.db/VoiceCommand_Event_Raw'
