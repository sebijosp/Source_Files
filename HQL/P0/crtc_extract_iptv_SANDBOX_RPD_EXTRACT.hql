set hive.execution.engine=tez;
set mapreduce.task.timeout=360000000;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers=none;
set hive.optimize.sort.dynamic.partition=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

!echo Parms to Hive Script: START_DATE=${hiveconf:START_DATE}  END_DATE=${hiveconf:END_DATE} ;

!echo Creating table if not exists :: SANDBOX.RPD_EXTRACT;

CREATE TABLE if not exists SANDBOX.RPD_EXTRACT
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
ASSET_CLASS VARCHAR(50)
)
STORED AS ORC;

!echo loading iptv Linear data load on table :: SANDBOX.RPD_EXTRACT;

Insert INTO TABLE SANDBOX.RPD_EXTRACT
select A.rogers_account_id,A.rogers_device_id,null,c.call_sign,A.session_start_ts,A.session_end_ts,hour(A.session_end_ts - A.session_start_ts)*3600+minute(A.session_end_ts - A.session_start_ts)*60+second(A.session_end_ts - A.session_start_ts) as Duration_sec,hour(A.session_end_ts - A.session_start_ts)*60+minute(session_end_ts - session_start_ts) as Duration_min,null,null,A.session_end_ts,null,a.COMPASS_STREAM_ID,a.asset_class_cd
from IPTV.IP_PLAYBACK_SUMFCT A join (SELECT DISTINCT CAN FROM SANDBOX.SAF_EXTRACT) B on trim(A.rogers_account_id)=trim(B.CAN)
join iptv.channel_dim c on A.compass_stream_id=c.source_id
where upper(a.asset_class_cd) in ('LINEAR')
and to_date(A.event_date) between '2018-09-09' and '2018-10-08';
-- and to_date(A.event_date) between '${hiveconf:START_DATE}' and '${hiveconf:END_DATE}';

!echo loading iptv CDVR data load on table :: SANDBOX.RPD_EXTRACT;

Insert INTO TABLE SANDBOX.RPD_EXTRACT
select A.rogers_account_id,A.rogers_device_id,null,d.call_sign,A.session_start_ts,A.session_end_ts,hour(A.session_end_ts - A.session_start_ts)*3600+minute(A.session_end_ts - A.session_start_ts)*60+second(A.session_end_ts - A.session_start_ts) as Duration_sec,hour(A.session_end_ts - A.session_start_ts)*60+minute(session_end_ts - session_start_ts) as Duration_min,null,null,A.session_end_ts,null,a.COMPASS_STREAM_ID,a.asset_class_cd
from IPTV.IP_PLAYBACK_SUMFCT A join (SELECT DISTINCT CAN FROM SANDBOX.SAF_EXTRACT) B on trim(A.rogers_account_id)=trim(B.CAN)
join iptv.dvr_schedule_dim c on trim(regexp_extract(a.merlin_recording_id,'.*L([0-9]+)S?\d*',1))=c.PROGRAM_LISTING_ID 
join iptv.channel_dim d on trim(regexp_extract(d.x1_station_id,'.*:([0-9]+)?\d*',1))=c.station_id
where upper(a.asset_class_cd) in ('CDVR')
and to_date(A.event_date) between '2018-09-09' and '2018-10-08';
-- and to_date(A.event_date) between '${hiveconf:START_DATE}' and '${hiveconf:END_DATE}';

!echo iptv RPD has completed successfully;
