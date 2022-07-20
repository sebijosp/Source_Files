set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

DROP TABLE IF EXISTS iptv.NEW_STB_INSTALLS;	
CREATE TABLE iptv.NEW_STB_INSTALLS(
  dt_monthly date,
  stbmodel varchar(50),
  newinstallcount decimal(6,0),
  extractmonth varchar(10))
STORED AS ORC;


drop table if exists  ext_iptv.NEW_STB_INSTALLS;

CREATE TABLE ext_IPTV.NEW_STB_INSTALLS 
   (
   DT_MONTHLY VARCHAR(50)
   ,STBMODEL VARCHAR(50)
   ,NEWINSTALLCOUNT VARCHAR(50)
   ,EXTRACTMONTH VARCHAR(50)
   )
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
"separatorchar" = "," ,
"quotechar" = "\"" 
)
stored as textfile
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '${hiveconf:file_name}' OVERWRITE INTO TABLE ext_iptv.NEW_STB_INSTALLS;

INSERT overwrite  TABLE iptv.NEW_STB_INSTALLS
select 
date_format(from_unixtime(unix_timestamp(dt_monthly, 'mm/dd/yy')), 'YYYY-DD-MM')
,STBMODEL 
,NEWINSTALLCOUNT 
,EXTRACTMONTH 
from ext_iptv.NEW_STB_INSTALLS;
