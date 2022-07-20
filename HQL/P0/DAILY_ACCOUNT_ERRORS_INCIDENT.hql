
set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

drop table if exists ext_iptv.DAILY_ACCOUNT_ERRORS;

CREATE TABLE ext_IPTV.DAILY_ACCOUNT_ERRORS
   (
dt_monthly string ,
dt_daily string ,
unique_error_count string ,
extractmonth string
   )
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
"separatorchar" = "," ,
"quotechar" = "\"" 
)
stored as textfile
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '${hiveconf:file_name}' OVERWRITE INTO TABLE ext_iptv.DAILY_ACCOUNT_ERRORS;

INSERT OVERWRITE TABLE IPTV.DAILY_ACCOUNT_ERRORS PARTITION(dt_monthly) SELECT 
CAST(from_unixtime(unix_timestamp(dt_daily , 'dd/MM/yy'),'yyyy-MM-dd') AS DATE) as dt_daily , 
translate(unique_error_count , '\,' , '') as unique_error_count ,  
extractmonth ,
to_date(from_unixtime(unix_timestamp(dt_monthly , 'dd/MM/yy'),'yyyy-MM-dd')) as dt_monthly 
from ext_iptv.DAILY_ACCOUNT_ERRORS ;

