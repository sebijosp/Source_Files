set hive.execution.engine=tez;
set mapreduce.task.timeout=360000000;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers=none;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

!echo Parms to Hive Script: START_DATE=${hiveconf:START_DATE}  END_DATE=${hiveconf:END_DATE} ;

!echo dropping table if exists :: CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM;

drop table if exists CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM;

!echo Creating table if not exists :: CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM;

CREATE TABLE if not exists CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM
(
PROGRAM_LISTING_ID DECIMAL(25,0),
REF_RECORDING_ID VARCHAR(200),
PROGRAM_TITLE_NM VARCHAR(300),
PROGRAM_START_TS TIMESTAMP,
LISTING_ID DECIMAL(25,0),
STATION_ID DECIMAL(25,0),
DURATION BIGINT
)
STORED AS ORC;

!echo loading iptv Linear data load on table :: CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM;

Insert INTO TABLE CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM
SELECT
program_listing_id,ref_recording_id,program_title_nm,program_start_ts,listing_id,station_id,duration
FROM IPTV.DVR_SCHEDULE_DIM 
where event_date >= date_sub('${hiveconf:END_DATE}', 30);
-- where event_date >= date_sub('2020-05-06', 30);

!echo dropping table if exists :: CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM_CORUS;

drop table if exists CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM_CORUS;

!echo Creating table the CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM_CORUS table which will contain only IPTV Viewership details: CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM_CORUS;

CREATE TABLE if not exists CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM_CORUS
(
PROGRAM_LISTING_ID DECIMAL(25,0),
REF_RECORDING_ID VARCHAR(200),
PROGRAM_TITLE_NM VARCHAR(300),
PROGRAM_START_TS TIMESTAMP,
LISTING_ID DECIMAL(25,0),
STATION_ID DECIMAL(25,0),
DURATION BIGINT
)
STORED AS ORC;


Insert INTO TABLE CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM_CORUS
SELECT
program_listing_id,ref_recording_id,program_title_nm,program_start_ts,listing_id,station_id,duration 
FROM
CRTC_VIEWERSHIP.DVR_SCHEDULE_DIM;

!echo IPTV DVR Extract completed successfully;
