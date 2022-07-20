drop table if exists iptv.ip_playback_sumfct_delta;
create table iptv.ip_playback_sumfct_delta as
SELECT
uuid                        AS uuid,
rogers_account_id           AS rogers_account_id,
comcast_account_id          AS comcast_account_id,
rogers_device_id            AS rogers_device_id,
comcast_device_id           AS comcast_device_id,
comcast_session_id          AS comcast_session_id,
session_start_ts            AS session_start_ts,
session_end_ts              AS session_end_ts,
app_name                    AS app_name,
app_version_num             AS app_version_num,
ipaddress_num               AS ipaddress_num,
isp_name                    AS isp_name,
network_location_name       AS network_location_name,
user_agent_name             AS user_agent_name,
asset_id                    AS asset_id,
asset_class_cd              AS asset_class_cd,
media_guid                  AS media_guid,
provider_id                 AS provider_id,
asset_content_id            AS asset_content_id,
program_id                  AS program_id,
merlin_recording_id         AS merlin_recording_id,
merlin_stream_id            AS merlin_stream_id,
compass_stream_id           AS compass_stream_id,
session_duration_count      AS session_duration_count,
completion_status_cd        AS completion_status_cd,
playback_type_cd            AS playback_type_cd,
trickplay_rewind_count      AS trickplay_rewind_count,
trickplay_ff_count          AS trickplay_ff_count,
trickplay_pause_count       AS trickplay_pause_count,
trickplay_play_count        AS trickplay_play_count,
playback_started_flag       AS playback_started_flag,
first_asset_position_num    AS first_asset_position_num,
last_asset_position_num     AS last_asset_position_num,
drm_latency_num             AS drm_latency_num,
open_latency_num            AS open_latency_num,
trickplay_events_num        AS trickplay_events_num,
fragment_warning_events_num AS fragment_warning_events_num,
frame_rate_min_num          AS frame_rate_min_num,
frame_rate_max_num          AS frame_rate_max_num,
bit_rate_min_num            AS bit_rate_min_num,
bit_rate_max_num            AS bit_rate_max_num,
event_date,
hdp_create_ts               AS hdp_create_ts,
hdp_update_ts               AS hdp_update_ts,
session_start_ts_local,
session_end_ts_local
FROM   iptv.ip_playback_sumfct
where event_date >= cast(date_add(cast(current_date as String), -30) as String)
and hdp_update_ts > '${hiveconf:DeltaPartStartTs}'
AND upper(asset_class_cd)='LINEAR'; 


drop table if exists iptv.ltv_comcast_str_stg_dim_temp;
CREATE TABLE iptv.ltv_comcast_str_stg_dim_temp AS
select * from
(SELECT
A.uuid,
A.rogers_account_id,
A.comcast_account_id,
A.rogers_device_id,
A.comcast_device_id,
A.comcast_session_id,
A.session_start_ts,
A.session_end_ts,
A.app_name,
A.app_version_num,
A.ipaddress_num,
A.isp_name,
A.network_location_name,
A.user_agent_name,
A.asset_id,
A.asset_class_cd,
A.media_guid,
A.provider_id,
A.asset_content_id,
A.program_id,
A.merlin_recording_id,
A.merlin_stream_id,
A.compass_stream_id,
A.session_duration_count,
A.completion_status_cd,
A.playback_type_cd,
A.trickplay_rewind_count,
A.trickplay_ff_count,
A.trickplay_pause_count,
A.trickplay_play_count,
A.playback_started_flag,
A.first_asset_position_num,
A.last_asset_position_num,
A.drm_latency_num,
A.open_latency_num,
A.trickplay_events_num,
A.fragment_warning_events_num,
A.frame_rate_min_num,
A.frame_rate_max_num,
A.bit_rate_min_num,
A.bit_rate_max_num,
A.hdp_create_ts,
A.hdp_update_ts,
D.subscriber_key,
D.subscriber_no                           AS subscriber1,
D.subscriber_seq_nbr,
D.l9_subcriber_sub_type,
D.address_key,
E.data_mac_id,
B.gracenote_id,
B.source_id,
E.customer_id                             AS EQUIP_CUSTOMER_ID,
B.channel_sk,
F.customer_key,
F.customer_id                             AS MEASTRO_CUSTOMER_ID,
C.subscriber_no,
C.customer_id                             AS SUBS_CUSTOMER_ID,
A.session_start_ts_local,
A.session_end_ts_local,
A.event_date,
Row_number() OVER(
partition BY A.uuid
ORDER BY A.session_start_ts DESC) AS DUP_FILTER
FROM   iptv.ip_playback_sumfct_delta A
LEFT JOIN iptv.ltv_channel_dim B
ON A.compass_stream_id = B.source_id
AND B.crnt_flg = 'Y'
LEFT JOIN iptv.subscriber C
ON A.rogers_account_id = C.prim_resource_val
AND C.crnt_f = 'Y'
AND C.subscriber_type = 'IP'
AND C.sub_status = 'A'
LEFT JOIN app_maestro.sbscrbrdim D
ON C.subscriber_no = D.subscriber_no
AND D.crnt_f = 'Y'
LEFT JOIN iptv.equipment_iptv_dim E
ON E.customer_id = C.customer_id
AND Regexp_replace(A.rogers_device_id, ':', '') =
E.data_mac_id
LEFT JOIN app_maestro.custdim F
ON F.customer_id = C.customer_id
AND F.crnt_f = 'Y')a
where DUP_FILTER=1;


insert into iptv.ltv_str_stg
select a.UUID, a.ROGERS_ACCOUNT_ID, a.COMCAST_ACCOUNT_ID, a.ROGERS_DEVICE_ID, a.COMCAST_DEVICE_ID, a.COMCAST_SESSION_ID, a.SESSION_START_TS, a.SESSION_END_TS, a.APP_NAME, a.APP_VERSION_NUM, a.IPADDRESS_NUM, a.ISP_NAME, a.NETWORK_LOCATION_NAME, a.USER_AGENT_NAME, a.ASSET_ID, a.ASSET_CLASS_CD, a.MEDIA_GUID, a.PROVIDER_ID, a.ASSET_CONTENT_ID, a.PROGRAM_ID, a.MERLIN_RECORDING_ID, a.MERLIN_STREAM_ID, a.COMPASS_STREAM_ID, a.SESSION_DURATION_COUNT, a.COMPLETION_STATUS_CD, a.PLAYBACK_TYPE_CD, a.PLAYBACK_STARTED_FLAG, a.FIRST_ASSET_POSITION_NUM, a.LAST_ASSET_POSITION_NUM,
a.DRM_LATENCY_NUM, a.OPEN_LATENCY_NUM, a.TRICKPLAY_EVENTS_NUM, a.FRAGMENT_WARNING_EVENTS_NUM, a.FRAME_RATE_MIN_NUM, a.FRAME_RATE_MAX_NUM, a.BIT_RATE_MIN_NUM, a.BIT_RATE_MAX_NUM,
a.HDP_CREATE_TS, a.HDP_UPDATE_TS, a.CHANNEL_SK, a.SUBSCRIBER_KEY, a.SUBSCRIBER_NO, a.SUBSCRIBER_SEQ_Nbr,a.L9_SUBCRIBER_SUB_TYPE, a.CUSTOMER_KEY, a.MEASTRO_CUSTOMER_ID, a.ADDRESS_KEY, a.DATA_MAC_ID,
concat_ws("|",case when a.ROGERS_ACCOUNT_ID IS NULL THEN '55' end,
case when UPPER(a.APP_NAME) like '.*STB.*' and a.ROGERS_DEVICE_ID is null then '54' end ,
case when a.SESSION_START_TS is null then '38'  end,
case when a.COMPASS_STREAM_ID is null then '53'  end ,
case when a.SUBSCRIBER_KEY is null then '11'  end ,
case when a.MEASTRO_CUSTOMER_ID is null and a.DATA_MAC_ID is null then '13'  end ,
case when a.CUSTOMER_KEY is null then '39' end, 
case when a.SOURCE_ID is null then '52'  end )as error_code
, current_date as FIRST_CREATE_DT,
b.date_id as SESSION_START_DATE_ID, c.time_id as SESSION_START_TIME_ID, a.SESSION_START_TS_LOCAL, a.SESSION_END_TS_LOCAL 
from iptv.ltv_comcast_str_stg_dim_temp a 
inner join app_shared_dim.LU_DATE b on to_date(a.SESSION_START_TS)=b.calendar_date 
inner join app_shared_dim.LU_TIME_INTERVAL c on substr(cast(a.SESSION_START_TS as string),12,8)=cast(c.time as string)
