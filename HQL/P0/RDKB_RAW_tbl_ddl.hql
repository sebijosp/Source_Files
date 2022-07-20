CREATE EXTERNAL TABLE `iptv.rdkb_raw`(
  `header` struct<`timestamp`:bigint,uuid:string,hostname:string,money:struct<traceid:string,spanid:bigint,parentspanid:bigint,spanname:string,appname:string,starttime:bigint,spanduration:bigint,spansuccess:boolean,notes:map<string,string>>> COMMENT 'from deserializer',
  `device` struct<receiverid:string,deviceid:string,devicesourceid:string,account:string,accountsourceid:string,billingaccountid:string,macaddress:string,ecmmacaddress:string,firmwareversion:string,devicetype:string,make:string,model:string,partner:string,ipaddress:string,utcoffset:int,postalcode:string,dma:string> COMMENT 'from deserializer',
  `timestamp` bigint COMMENT 'from deserializer',
  `rdkbevents` array<struct<eventcode:string,eventcount:int>> COMMENT 'from deserializer',
  `attributes` struct<loadavgsplit:float,loadavgatomsplit:float,uptimesplit:int,usedcpusplit:int,usedmemsplit:int,usedcpuatomsplit:int,usedmematomsplit:int,total2gclientssplit:int,total5gclientssplit:int,totalethernetclientssplit:int,totalmocaclientssplit:int,samessid:int,thermalchiptempsplit:float,thermalfanspeedsplit:float,thermalwifichiptempsplit:float,mocaenabled:int,mocadisabled:int,wifi2gutilizationsplit:int,wifi5gutilizationsplit:int,bandsteer2gto5greasonrssi:int,bandsteer2gto5greasonutilization:int,bandsteer5gto2greasonrssi:int,bandsteer5gto2greasonutilization:int,bandsteerpreassociationto2g:int,bandsteerpreassociationto5g:int,bootuptimemocasplit:int,bootuptimewebpasplit:int,bootuptimeethernetsplit:int,bootuptimewifisplit:int,bootuptimexhomesplit:int,bsoffrdkb:int,erouteripv4:string,erouteripv6:string> COMMENT 'from deserializer',rawAttributes MAP<STRING,STRING>,
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
  '/apps/hive/warehouse/iptv.db/rdkb_raw';
  
