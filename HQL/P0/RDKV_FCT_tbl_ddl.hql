CREATE EXTERNAL TABLE `iptv.rdkv_fct`(
  `header` struct<`timestamp`:bigint,uuid:string,hostname:string,money:struct<traceid:string,spanid:bigint,parentspanid:bigint,spanname:string,appname:string,starttime:bigint,spanduration:bigint,spansuccess:boolean,notes:map<string,string>>> COMMENT 'from deserializer',
  `device` struct<receiverid:string,deviceid:string,devicesourceid:string,account:string,accountsourceid:string,billingaccountid:string,macaddress:string,ecmmacaddress:string,firmwareversion:string,devicetype:string,make:string,model:string,partner:string,ipaddress:string,utcoffset:int,postalcode:string,dma:string> COMMENT 'from deserializer',
  `timestamp` bigint COMMENT 'from deserializer',
  `rdkvevents` array<struct<eventcode:string,eventcount:int>> COMMENT 'from deserializer',
  `attributes` struct<loadaverage:float,memreceiver:string,cpureceiver:float,memrmfstreamer:string,cpurmfstreamer:float,memrunpod:string,cpurunpod:float,stbip:string,channelmapid:int,controllerid:int,vctid:string,vodserverid:string,xisignalstrength:int,xinoise:int,xiconnectedssid:string,xiconnectionstatus:string> COMMENT 'from deserializer',rawAttributes MAP<STRING,STRING>,
  `hdp_file_name` string COMMENT 'from deserializer',
  `hdp_create_ts` string COMMENT 'from deserializer',
  `hdp_update_ts` string COMMENT 'from deserializer')
PARTITIONED BY (
  `event_date` date)
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json'='true')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/apps/hive/warehouse/iptv.db/rdkv_fct';
