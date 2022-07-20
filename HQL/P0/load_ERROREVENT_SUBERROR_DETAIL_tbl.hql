!echo ;
!echo Hive : Dropping External Table IPTV.ERROREVENT_SUBERROR_DETAIL_TEMP;
DROP TABLE IF EXISTS IPTV.ERROREVENT_SUBERROR_DETAIL_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table IPTV.ERROREVENT_SUBERROR_DETAIL_TEMP at '/userapps/iptv/errorevent_suberror_detail';

create external table IPTV.ERROREVENT_SUBERROR_DETAIL_TEMP(
    `ERROR_CODE` string,
    `SUB_ERROR_CODE` string,
    `SUB_ERROR_NAME` string,
    `DESCRIPTION` string )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/iptv/errorevent_suberror_detail'
tblproperties ("skip.header.line.count"="1");

!echo ;
!echo Hive : Temp Table creation successfull;

CREATE TABLE if not exists `iptv.errorevent_suberror_detail`
  (
    `error_code` string,
    `sub_error_code` string,
    `sub_error_name` string,
    `description` string,
    `hdp_insert_ts` TIMESTAMP,
    `hdp_update_ts` TIMESTAMP
  )stored as ORC;

!echo Hive : Loading data into table IPTV.ERROREVENT_SUBERROR_DETAIL;

insert overwrite table IPTV.ERROREVENT_SUBERROR_DETAIL
select
trim(ERROR_CODE) ERROR_CODE ,
trim(SUB_ERROR_CODE) SUB_ERROR_CODE ,
SUB_ERROR_NAME ,
DESCRIPTION ,
from_unixtime(unix_timestamp()),
from_unixtime(unix_timestamp())
FROM IPTV.ERROREVENT_SUBERROR_DETAIL_TEMP;

!echo ;
!echo Hive : Data load successful into IPTV.ERROREVENT_SUBERROR_DETAIL;
