
set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

drop table if exists ext_iptv.DAILY_ACTIVE_ACCOUNTS;

CREATE TABLE ext_IPTV.DAILY_ACTIVE_ACCOUNTS
   (
dt_monthly string ,
dt_daily string ,
accountcount string ,
devicecount string,
extractmonth string
   )
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
"separatorchar" = "," ,
"quotechar" = "\"" 
)
stored as textfile
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '${hiveconf:file_name}' OVERWRITE INTO TABLE ext_iptv.DAILY_ACTIVE_ACCOUNTS;

INSERT OVERWRITE TABLE IPTV.DAILY_ACTIVE_ACCOUNTS PARTITION(dt_monthly) SELECT 
CAST(from_unixtime(unix_timestamp(dt_daily , 'dd/MM/yy'),'yyyy-MM-dd') AS DATE) as dt_daily , 
translate(accountcount , '\,' , '') as accountcount , 
translate(devicecount , '\,' , '') as devicecount , 
extractmonth ,
to_date(from_unixtime(unix_timestamp(dt_monthly , 'dd/MM/yy'),'yyyy-MM-dd')) as dt_monthly 
from ext_iptv.DAILY_ACTIVE_ACCOUNTS ;
