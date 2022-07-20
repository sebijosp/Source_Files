set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

drop table if exists iptv.IPTV_STB_ERRORS_CLASSIFICATION;

CREATE TABLE IPTV.IPTV_STB_ERRORS_CLASSIFICATION
   (
  error_code string ,
  title string ,
  error_code_type string ,
  description string ,
  customer_self_help_text string ,
  care_instructions string ,
  current_message_copy string ,
  possible_causes string ,
  tier_2_instructions string,
  created_dt string ,
  updated_dt string 
   )
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
"separatorchar" = "," ,
"quotechar" = "\"" 
)
stored as textfile
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '${hiveconf:file_name}' OVERWRITE INTO TABLE iptv.IPTV_STB_ERRORS_CLASSIFICATION;
