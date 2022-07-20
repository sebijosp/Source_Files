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

!echo dropping table if exists :: CRTC_VIEWERSHIP.VOD_ASSET_DIM;

drop table if exists CRTC_VIEWERSHIP.VOD_ASSET_DIM;


!echo Creating table if not exists :: CRTC_VIEWERSHIP.VOD_ASSET_DIM;

CREATE TABLE if not exists CRTC_VIEWERSHIP.VOD_ASSET_DIM
(
ASSET_ID VARCHAR(50),
PACKAGE_ID VARCHAR(20),
PROVIDER_NAME VARCHAR(100),
ASSET_NAME VARCHAR(100),
ASSET_DESC VARCHAR(4000),
ASSET_CLASS VARCHAR(20),
EPISODE_NAME VARCHAR(100),
EPISODE_ID VARCHAR(20),
CREATION_TS DATE,
IS_ACTIVE_FLAG CHAR(1),
GENRE VARCHAR(100),
ASSET_TYPE_CD VARCHAR(20),
VOD_TYPE_CD VARCHAR(20),
PROGRAM_NAME VARCHAR(4000),
RUN_TIME_SEC   BIGINT,
LICENSING_WINDOW_START timestamp,
LICENSING_WINDOW_END timestamp,
LANGUAGES  varchar(100)

)
STORED AS ORC;

!echo loading iptv Linear data load on table :: CRTC_VIEWERSHIP.VOD_ASSET_DIM;

Insert INTO TABLE CRTC_VIEWERSHIP.VOD_ASSET_DIM
SELECT
asset_id,package_id,provider_name,asset_name,asset_desc,asset_class,episode_name,case when episode_id is null then regexp_extract(asset_name, '(S[0-9]{1,}E[0-9]{1,})', 1) 
  else episode_id end as episode_id,creation_ts,is_active_flag,genre,asset_type_cd,vod_type_cd,case 
when Substr(asset_desc, 4, 1) >= 0 and 
  (Substr(asset_desc, 1, 3) = date_format(creation_ts, 'MMM') Or  Substr(asset_desc, 1, 3) = date_format(add_months(creation_ts, 1), 'MMM') Or  Substr(asset_desc, 1, 3) = date_format(add_months(creation_ts, -1), 'MMM') ) then 
  lTrim(regexp_replace(REGEXP_REPLACE(regexp_replace(Substr(asset_desc, 4, length(asset_desc)-3),'^[0-9]{4,}+', ''), '(S[0-9]{1,}E[0-9]{1,})|(S[0-9]{1,}Ep[0-9]{1,})|(Ep[0-9]{1,})|(S.[0-9]{1,} E.[0-9]{1,})|(Ep.[0-9]{1,})|(Episode[0-9]{1,})|(Season[0-9]{1,})|(Episode [0-9]{1,})|(Season [0-9]{1,})', ''),'(?<=[a-z])([A-Z])|(?<=[0-9])([A-Z])', ' $0'))
when length(regexp_extract(asset_desc, '[0-9]+', 0)) >= 4 and Substr(regexp_extract(asset_desc, '[0-9]+', 0), 1, 2) <=12 then 
  lTrim(regexp_replace(REGEXP_REPLACE(regexp_replace(asset_desc,'^[0-9]{4,}+', ''), '(S[0-9]{1,}E[0-9]{1,})|(S[0-9]{1,}Ep[0-9]{1,})|(Ep[0-9]{1,})|(S.[0-9]{1,} E.[0-9]{1,})|(Ep.[0-9]{1,})|(Episode[0-9]{1,})|(Season[0-9]{1,})|(Episode [0-9]{1,})|(Season [0-9]{1,})', ''),'(?<=[a-z])([A-Z])|(?<=[0-9])([A-Z])', ' $0'))
when length(regexp_extract(asset_desc, '[0-9]+', 0)) >= 6 Then 
  lTrim(regexp_replace(REGEXP_REPLACE(regexp_replace(asset_desc,'^[0-9]{4,}+', ''), '(S[0-9]{1,}E[0-9]{1,})|(S[0-9]{1,}Ep[0-9]{1,})|(Ep[0-9]{1,})|(S.[0-9]{1,} E.[0-9]{1,})|(Ep.[0-9]{1,})|(Episode[0-9]{1,})|(Season[0-9]{1,})|(Episode [0-9]{1,})|(Season [0-9]{1,})', ''),'(?<=[a-z])([A-Z])|(?<=[0-9])([A-Z])', ' $0'))
else 
  lTrim(regexp_replace(REGEXP_REPLACE(asset_desc, '(S[0-9]{1,}E[0-9]{1,})|(S[0-9]{1,}Ep[0-9]{1,})|(Ep[0-9]{1,})|(S.[0-9]{1,} E.[0-9]{1,})|(Ep.[0-9]{1,})|(Episode[0-9]{1,})|(Season[0-9]{1,})|(Episode [0-9]{1,})|(Season [0-9]{1,})', ''),'(?<=[a-z])([A-Z])|(?<=[0-9])([A-Z])', ' $0'))
end as program_name, cast(split(run_time, ':')[0] * 3600 + 
            split(run_time, ':')[1] * 60 + 
            split(run_time, ':')[2] as int) as run_time_secs
, licensing_window_start
, licensing_window_End
, Languages
FROM IPTV.VOD_ASSET_DIM
where to_date(hdp_create_ts) between '${hiveconf:START_DATE}' and '${hiveconf:END_DATE}';
-- Where to_date(hdp_create_ts) > '2017-10-15';

!echo dropping table if exists :: CRTC_VIEWERSHIP.VOD_ASSET_DIM_CORUS;

drop table if exists CRTC_VIEWERSHIP.VOD_ASSET_DIM_CORUS;

!echo Creating table the CRTC_VIEWERSHIP.VOD_ASSET_DIM_CORUS table which will contain only IPTV Viewership details: CRTC_VIEWERSHIP.VOD_ASSET_DIM_CORUS;

CREATE TABLE if not exists CRTC_VIEWERSHIP.VOD_ASSET_DIM_CORUS
(
ASSET_ID VARCHAR(50),
PACKAGE_ID VARCHAR(20),
PROVIDER_NAME VARCHAR(100),
ASSET_NAME VARCHAR(100),
ASSET_DESC VARCHAR(4000),
ASSET_CLASS VARCHAR(20),
EPISODE_NAME VARCHAR(100),
EPISODE_ID VARCHAR(20),
CREATION_TS DATE,
IS_ACTIVE_FLAG CHAR(1),
GENRE VARCHAR(100),
ASSET_TYPE_CD VARCHAR(20),
VOD_TYPE_CD VARCHAR(20),
PROGRAM_NAME VARCHAR(4000),
RUN_TIME_SEC   BIGINT,
LICENSING_WINDOW_START timestamp,
LICENSING_WINDOW_END timestamp,
LANGUAGES  varchar(100)

)
STORED AS ORC;


Insert INTO TABLE CRTC_VIEWERSHIP.VOD_ASSET_DIM_CORUS
SELECT
asset_id,package_id,provider_name,asset_name,asset_desc,asset_class,episode_name,episode_id,creation_ts,is_active_flag,genre,asset_type_cd,vod_type_cd, Program_name, Run_time_sec,  licensing_window_start, licensing_window_End, Languages
FROM
CRTC_VIEWERSHIP.VOD_ASSET_DIM;

!echo IPTV VOD Extract completed successfully;
