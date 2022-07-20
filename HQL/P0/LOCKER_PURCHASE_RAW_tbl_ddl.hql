Drop table `iptv`.`locker_purchase_raw`;
CREATE EXTERNAL TABLE `iptv`.`locker_purchase_raw`(
  `header` struct<uuid:string,`timestamp`:bigint,hostname:string,money:struct<traceid:string,spanid:bigint,parentspanid:bigint,spanname:string,appname:string,starttime:bigint,spanduration:bigint,spansuccess:boolean,notes:map<string,string>>> COMMENT 'from deserializer',
  `partner` string COMMENT 'from deserializer',
  `device` struct<receiverid:string,deviceid:string,devicesourceid:string,account:string,accountsourceid:string,billingaccountid:string,macaddress:string,ecmmacaddress:string,firmwareversion:string,devicetype:string,make:string,model:string,partner:string,ipaddress:string,utcoffset:string,postalcode:string,dma:string> COMMENT 'from deserializer',
  `action` string COMMENT 'from deserializer',
  `transactionid` string COMMENT 'from deserializer',
  `partnertransactionid` string COMMENT 'from deserializer',
  `transactiontime` bigint COMMENT 'from deserializer',
  `description` string COMMENT 'from deserializer',
  `offerid` bigint COMMENT 'from deserializer',
  `pricetopurchaser` string COMMENT 'from deserializer',
  `currencycode` string COMMENT 'from deserializer',
  `preorder` boolean COMMENT 'from deserializer',
  `revokereason` string COMMENT 'from deserializer',
  `revoketimestamp` bigint COMMENT 'from deserializer',
  `assetright` struct<assetid:string,title:string,starttime:bigint,endtime:bigint,purchasecategory:string> COMMENT 'from deserializer',
  `hdp_file_name` string COMMENT 'from deserializer',
  `hdp_create_ts` timestamp COMMENT 'from deserializer',
  `hdp_update_ts` timestamp COMMENT 'from deserializer')
PARTITIONED BY (
  `received_date` date)
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json'='true')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  


