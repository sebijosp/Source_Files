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

!echo Creating table if not exists :: CRTC_VIEWERSHIP.RPD_EXTRACT;

CREATE TABLE if not exists CRTC_VIEWERSHIP.RPD_EXTRACT
(
CAN VARCHAR(20),
DEVICE_ID VARCHAR(40),
CHANNEL INT,
CHANNEL_DESC VARCHAR(50),
START_VIEW TIMESTAMP,
END_VIEW TIMESTAMP,
DURATION_SEC INT,
DURATION_MIN INT,
IPG_CHANNEL VARCHAR(50),
IS_SDV INT,
N_HOUR_END_TIME TIMESTAMP,
SAM_CHANNEL VARCHAR(50),
SOURCE_ID INT,
ASSET_CLASS VARCHAR(50),
ASSET_ID VARCHAR(50),
RECORDING_ID VARCHAR(100),
STATION_ID VARCHAR(100),
GRACENOTE_ID VARCHAR(50),
PRODUCT_SOURCE VARCHAR(50)
)
STORED AS ORC;

!echo loading iptv Linear data load on table :: CRTC_VIEWERSHIP.RPD_EXTRACT;

Insert INTO TABLE CRTC_VIEWERSHIP.RPD_EXTRACT
 select t1.rogers_account_id,t1.rogers_device_id,null,t1.call_sign,t1.session_start_ts,t1.session_end_ts,t1.Duration_sec,t1.Duration_min,null,null,t1.N_HOUR_END_TIME,null,t1.COMPASS_STREAM_ID,t1.asset_class_cd,t1.asset_id,t1.merlin_recording_id,t1.x1_station_id,t1.gracenote_id,'IPTV' as Product_source  
from
 (select A.rogers_account_id as rogers_account_id,A.rogers_device_id as rogers_device_id,null,c.call_sign as call_sign,A.session_start_ts as session_start_ts,A.session_end_ts as session_end_ts,hour(A.session_end_ts - A.session_start_ts)*3600+minute(A.session_end_ts - A.session_start_ts)*60+second(A.session_end_ts - A.session_start_ts) as Duration_sec,hour(A.session_end_ts - A.session_start_ts)*60+minute(session_end_ts - session_start_ts) as Duration_min,null,null,A.session_end_ts as N_HOUR_END_TIME,null,a.COMPASS_STREAM_ID  as COMPASS_STREAM_ID,a.asset_class_cd as asset_class_cd,A.asset_id as asset_id,A.merlin_recording_id as merlin_recording_id,c.x1_station_id as x1_station_id,c.gracenote_id as gracenote_id
from IPTV.IP_PLAYBACK_SUMFCT A join (SELECT DISTINCT CAN FROM CRTC_VIEWERSHIP.SAF_EXTRACT) B on trim(A.rogers_account_id)=trim(B.CAN)
join iptv.channel_dim c on A.compass_stream_id=c.source_id
where upper(a.asset_class_cd) in ('LINEAR', 'CDVR')
and to_date(A.event_date) >= date_sub('${hiveconf:START_DATE}', 15) and To_date(A.hdp_create_ts) between '${hiveconf:START_DATE}' and '${hiveconf:END_DATE}'
)t1 
group by t1.rogers_account_id,t1.rogers_device_id,null,t1.call_sign,t1.session_start_ts,t1.session_end_ts,t1.Duration_sec,t1.Duration_min,null,null,t1.N_HOUR_END_TIME,null,t1.COMPASS_STREAM_ID,t1.asset_class_cd,t1.asset_id,t1.merlin_recording_id,t1.x1_station_id,t1.gracenote_id;


!echo loading VOD data load on table :: CRTC_VIEWERSHIP.RPD_EXTRACT;


Insert INTO TABLE CRTC_VIEWERSHIP.RPD_EXTRACT
select t1.rogers_account_id,t1.rogers_device_id,null,t1.call_sign,t1.session_start_ts,t1.session_end_ts,t1.Duration_sec,t1.Duration_min,null,null,t1.N_HOUR_END_TIME,null,t1.COMPASS_STREAM_ID,t1.asset_class_cd,t1.asset_id,t1.merlin_recording_id,t1.x1_station_id,t1.gracenote_id,'IPTV' as Product_source
from
(select A.rogers_account_id as rogers_account_id,A.rogers_device_id as rogers_device_id,null,null as call_sign,A.session_start_ts as session_start_ts,A.session_end_ts as session_end_ts,hour(A.session_end_ts - A.session_start_ts)*3600+minute(A.session_end_ts - A.session_start_ts)*60+second(A.session_end_ts - A.session_start_ts) as Duration_sec,hour(A.session_end_ts - A.session_start_ts)*60+minute(session_end_ts - session_start_ts) as Duration_min,null,null,A.session_end_ts as N_HOUR_END_TIME,null,a.COMPASS_STREAM_ID  as COMPASS_STREAM_ID,a.asset_class_cd as asset_class_cd,A.asset_id as asset_id,A.merlin_recording_id as merlin_recording_id,null as x1_station_id,null as gracenote_id
from IPTV.IP_PLAYBACK_SUMFCT A join (SELECT DISTINCT CAN FROM CRTC_VIEWERSHIP.SAF_EXTRACT) B on trim(A.rogers_account_id)=trim(B.CAN)
join iptv.vod_asset_dim d on a.asset_id=d.asset_id
where upper(a.asset_class_cd) in ('VOD')
and to_date(A.event_date) >= date_sub('${hiveconf:START_DATE}',15) and To_date(A.hdp_create_ts) between '${hiveconf:START_DATE}' and '${hiveconf:END_DATE}'
)t1
group by t1.rogers_account_id,t1.rogers_device_id,null,t1.call_sign,t1.session_start_ts,t1.session_end_ts,t1.Duration_sec,t1.Duration_min,null,null,t1.N_HOUR_END_TIME,null,t1.COMPASS_STREAM_ID,t1.asset_class_cd,t1.asset_id,t1.merlin_recording_id,t1.x1_station_id,t1.gracenote_id;


!echo loading CDVR data load on table :: CRTC_VIEWERSHIP.RPD_EXTRACT;

Insert INTO TABLE CRTC_VIEWERSHIP.RPD_EXTRACT
select t1.rogers_account_id,t1.rogers_device_id,null,t1.call_sign,t1.session_start_ts,t1.session_end_ts,t1.Duration_sec,t1.Duration_min,null,null,t1.N_HOUR_END_TIME,null,t1.COMPASS_STREAM_ID,t1.asset_class_cd,t1.asset_id,t1.merlin_recording_id,t1.x1_station_id,t1.gracenote_id,'IPTV' as Product_source
from
(select A.rogers_account_id as rogers_account_id,A.rogers_device_id as rogers_device_id,null,d.call_sign as call_sign,A.session_start_ts as session_start_ts,A.session_end_ts as session_end_ts,hour(A.session_end_ts - A.session_start_ts)*3600+minute(A.session_end_ts - A.session_start_ts)*60+second(A.session_end_ts - A.session_start_ts) as Duration_sec,hour(A.session_end_ts - A.session_start_ts)*60+minute(session_end_ts - session_start_ts) as Duration_min,null,null,A.session_end_ts as N_HOUR_END_TIME,null,a.COMPASS_STREAM_ID as COMPASS_STREAM_ID,a.asset_class_cd as asset_class_cd,A.asset_id as asset_id,c.PROGRAM_LISTING_ID as merlin_recording_id,c.station_id as x1_station_id,d.gracenote_id as gracenote_id
from IPTV.IP_PLAYBACK_SUMFCT A join (SELECT DISTINCT CAN FROM CRTC_VIEWERSHIP.SAF_EXTRACT) B on trim(A.rogers_account_id)=trim(B.CAN)
join iptv.dvr_schedule_dim c on trim(regexp_extract(a.merlin_recording_id,'.*L([0-9]+)S?\d*',1))=c.PROGRAM_LISTING_ID
join iptv.channel_dim d on trim(regexp_extract(d.x1_station_id,'.*:([0-9]+)?\d*',1))=c.station_id
where upper(a.asset_class_cd) in ('CDVR')
and to_date(A.event_date) >= date_sub('${hiveconf:START_DATE}',15) and to_date(A.hdp_create_ts) between '${hiveconf:START_DATE}' and '${hiveconf:END_DATE}'
)t1
group by t1.rogers_account_id,t1.rogers_device_id,null,t1.call_sign,t1.session_start_ts,t1.session_end_ts,t1.Duration_sec,t1.Duration_min,null,null,t1.N_HOUR_END_TIME,null,t1.COMPASS_STREAM_ID,t1.asset_class_cd,t1.asset_id,t1.merlin_recording_id,t1.x1_station_id,t1.gracenote_id;

!echo dropping table if exists :: CRTC_VIEWERSHIP.RPD_EXTRACT_CORUS;

drop table if exists CRTC_VIEWERSHIP.RPD_EXTRACT_CORUS;

!echo Creating table the CRTC_VIEWERSHIP.RPD_EXTRACT_CORUS table which will contain only IPTV Viewership details:: CRTC_VIEWERSHIP.RPD_EXTRACT_CORUS;

CREATE TABLE if not exists CRTC_VIEWERSHIP.RPD_EXTRACT_CORUS
(
CAN VARCHAR(20),
DEVICE_ID VARCHAR(40),
CHANNEL INT,
CHANNEL_DESC VARCHAR(50),
START_VIEW TIMESTAMP,
END_VIEW TIMESTAMP,
DURATION_SEC INT,
DURATION_MIN INT,
IPG_CHANNEL VARCHAR(50),
IS_SDV INT,
N_HOUR_END_TIME TIMESTAMP,
SAM_CHANNEL VARCHAR(50),
SOURCE_ID INT,
ASSET_CLASS VARCHAR(50),
ASSET_ID VARCHAR(50),
RECORDING_ID VARCHAR(100),
STATION_ID VARCHAR(100),
GRACENOTE_ID VARCHAR(50)
)
STORED AS ORC;


Insert INTO TABLE CRTC_VIEWERSHIP.RPD_EXTRACT_CORUS
SELECT
can,device_id,channel,channel_desc,start_view,end_view,duration_sec,duration_min,ipg_channel,is_sdv,n_hour_end_time,sam_channel,source_id,asset_class,asset_id,recording_id,station_id,gracenote_id 
FROM
CRTC_VIEWERSHIP.RPD_EXTRACT 
where PRODUCT_SOURCE='IPTV';

!echo IPTV RPD Extract completed successfully;
