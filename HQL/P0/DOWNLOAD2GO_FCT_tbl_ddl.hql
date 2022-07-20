drop table if exists iptv.`download2go_fct`;
CREATE EXTERNAL TABLE iptv.`download2go_fct`(
  `header` struct<`timestamp`:bigint,uuid:string,hostname:string,money:struct<traceId:string,spanId:bigint,parentSpanId:bigint,spanName:string,appName:string,startTime:bigint,spanDuration:bigint,spanSuccess:boolean,notes:map<string,string>>> COMMENT 'from deserializer',
  `partnerId` string COMMENT 'from deserializer',
  `sessionId` string COMMENT 'from deserializer',
  `creationTime` bigint COMMENT 'from deserializer',
  `startTimes` array<bigint> COMMENT 'from deserializer',
  `endTime` bigint COMMENT 'from deserializer',
  `status` string COMMENT 'from deserializer',
  `device` struct<physicalDeviceId:string,serviceAccountId:string,deviceType:string,deviceName:string,deviceVersion:string,ipAddress:string,networkLocation:string,deviceSourceId:string,accountSourceId:string,customFields:map<string,string>> COMMENT 'from deserializer',
  `application` struct<applicationName:string,applicationVersion:string,playerName:string,playerVersion:string,pluginName:string,pluginVersion:string,userAgent:string,customFields:map<string,string>> COMMENT 'from deserializer',
  `asset` struct<assetClass:string,regulatoryClass:string,playbackType:string,mediaGuid:string,providerId:string,assetId:string,assetContentId:string,mediaId:string,platformId:string,recordingId:string,streamId:string,easUri:string,manifestUrl:string,virtualStream:struct<timing:struct<clientGeneratedTimestamp:bigint,clientPostTimestamp:bigint,serverReceivedTimestamp:bigint,clientTimestamp:bigint,position:int>,eventName:string,sourceId:string,signalId:string,serviceZone:string>,title:string,customFields:map<string,string>> COMMENT 'from deserializer',
  `totalFragments` int COMMENT 'from deserializer',
  `completedFragments` int COMMENT 'from deserializer',
  `resolution` string COMMENT 'from deserializer',
  `geolocation` struct<city:string,region:string,postalCode:string,country:string,latitude:double,longitude:double,dma:string,utcOffset:float> COMMENT 'from deserializer',
  `failures` array<struct<offset:bigint,errorDomain:string,errorDescription:string>> COMMENT 'from deserializer',
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
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


