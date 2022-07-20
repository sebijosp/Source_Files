drop table if exists iptv.download2go_raw;
CREATE EXTERNAL TABLE iptv.download2go_raw(
  header struct<timestamp:bigint,uuid:string,hostname:string,money:struct<traceid:string,spanid:bigint,parentspanid:bigint,spanname:string,appname:string,starttime:bigint,spanduration:bigint,spansuccess:boolean,notes:map<string,string>>> COMMENT 'from deserializer',
  partnerid string COMMENT 'from deserializer',
  sessionid string COMMENT 'from deserializer',
  creationtime bigint COMMENT 'from deserializer',
  starttimes array<bigint> COMMENT 'from deserializer',
  endtime bigint COMMENT 'from deserializer',
  status string COMMENT 'from deserializer',
  device struct<physicaldeviceid:string,serviceaccountid:string,devicetype:string,devicename:string,deviceversion:string,ipaddress:string,networklocation:string,devicesourceid:string,accountsourceid:string,customfields:map<string,string>> COMMENT 'from deserializer',
  application struct<applicationname:string,applicationversion:string,playername:string,playerversion:string,pluginname:string,pluginversion:string,useragent:string,customfields:map<string,string>> COMMENT 'from deserializer',
  asset struct<assetclass:string,regulatoryclass:string,playbacktype:string,mediaguid:string,providerid:string,assetid:string,assetcontentid:string,mediaid:string,platformid:string,recordingid:string,streamid:string,easuri:string,manifesturl:string,virtualstream:struct<timing:struct<clientgeneratedtimestamp:bigint,clientposttimestamp:bigint,serverreceivedtimestamp:bigint,clienttimestamp:bigint,position:int>,eventname:string,sourceid:string,signalid:string,servicezone:string>,title:string,customfields:map<string,string>> COMMENT 'from deserializer',
  totalfragments int COMMENT 'from deserializer',
  completedfragments int COMMENT 'from deserializer',
  resolution string COMMENT 'from deserializer',
  geolocation struct<city:string,region:string,postalcode:string,country:string,latitude:double,longitude:double,dma:string,utcoffset:float> COMMENT 'from deserializer',
  failures array<struct<offset:bigint,errordomain:string,errordescription:string>> COMMENT 'from deserializer',
  hdp_file_name string COMMENT 'from deserializer',
  hdp_create_ts string COMMENT 'from deserializer',
  hdp_update_ts string COMMENT 'from deserializer')
PARTITIONED BY (
  received_date date)
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json'='true')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
