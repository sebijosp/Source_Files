
set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

drop table if exists ext_iptv.ERRORS_PER_HOUR;

CREATE TABLE ext_IPTV.ERRORS_PER_HOUR
   (
dt_daily string ,
accounts string ,
hours string ,
hourspererroraccounts string ,
total_error_response_codes string ,
errorsperhour string ,
extractmonth string
   )
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
"separatorchar" = "," ,
"quotechar" = "\"" 
)
stored as textfile
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '${hiveconf:file_name}' OVERWRITE INTO TABLE ext_iptv.ERRORS_PER_HOUR;

INSERT OVERWRITE TABLE IPTV.ERRORS_PER_HOUR PARTITION(dt_monthly) SELECT 
CAST(from_unixtime(unix_timestamp(dt_daily , 'dd/MM/yy'),'yyyy-MM-dd') AS DATE) as dt_daily , 
translate(accounts , '\,' , '') as accounts , 
translate(hours , '\,' , '') as hours ,
translate(hourspererroraccounts , '\,' , '') as hourspererroraccounts  ,
translate(total_error_response_codes , '\,' , '') as total_error_response_codes , 
translate(errorsperhour , '\,' , '') as errorsperhour ,
extractmonth,
to_date(from_unixtime(unix_timestamp(dt_daily , 'dd/MM/yy'),'yyyy-MM-dd')) as dt_monthly 
from ext_iptv.ERRORS_PER_HOUR ;

