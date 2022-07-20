
set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

drop table if exists ext_iptv.MONTHLY_ACTIVE_TEST_ACCOUNTS;

CREATE TABLE ext_IPTV.MONTHLY_ACTIVE_TEST_ACCOUNTS
   (
dt_monthly string ,
testaccountcount string ,
testdevicecount string,
extractmonth string
   )
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
"separatorchar" = "," ,
"quotechar" = "\"" 
)
stored as textfile
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '${hiveconf:file_name}' OVERWRITE INTO TABLE ext_iptv.MONTHLY_ACTIVE_TEST_ACCOUNTS;

INSERT OVERWRITE TABLE IPTV.MONTHLY_ACTIVE_TEST_ACCOUNTS PARTITION(dt_monthly) SELECT 
translate(testaccountcount , '\,' , '') as testaccountcount , 
translate(testdevicecount , '\,' , '') as testdevicecount , 
extractmonth ,
to_date(from_unixtime(unix_timestamp(dt_monthly , 'dd/MM/yy'),'yyyy-MM-dd')) as dt_monthly 
from ext_iptv.MONTHLY_ACTIVE_TEST_ACCOUNTS ;
