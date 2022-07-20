!echo ;
!echo Hive : Drop Partitions;


Alter table iptv.ltv_stream_fact_az drop if exists partition (event_date > '1970-01-01');

!echo ;
!echo Hive :Insert into the table;


Insert overwrite  table  iptv.ltv_stream_fact_az Partition(event_date)
select a.ltv_sk, a.uuid, a.rogers_account_id,    a.comcast_account_id,   default.tokenize(a.rogers_device_id, 'K10') as rogers_device_id ,     a.comcast_device_id,    a.comcast_session_id,   a.session_start_ts_utc, a.session_end_ts_utc,   a.app_name,     a.app_version_num,      default.tokenize(a.ipaddress_num, 'K10') as ipaddress_num,        a.isp_name,     a.network_location_name,        a.user_agent_name,      a.asset_id,     a.asset_class_cd,       a.media_guid,   a.provider_id,  a.asset_content_id,     a.program_id,   a.merlin_recording_id,  a.merlin_stream_id,     a.compass_stream_id,    a.session_duration_count,       a.completion_status_cd, a.playback_type_cd,     a.playback_started_flag,        a.first_asset_position_num,     a.last_asset_position_num,      a.drm_latency_num,      a.open_latency_num,     a.trickplay_events_num, a.fragment_warning_events_num,  a.frame_rate_min_num,   a.frame_rate_max_num,   a.bit_rate_min_num,     a.bit_rate_max_num,     a.session_start_date_id,        a.session_start_time_id,        a.session_stop_date_id, a.session_stop_time_id, a.prg_station_id,       a.gn_program_id,        a.airing_start_date_id, a.airing_start_time_id, a.channel_sk,   a.subscriber_key,       a.subscriber_no,        a.subscriber_seq_no,    a.l9_subcriber_sub_type,        a.customer_key, a.customer_id,  a.address_key,  a.data_mac_id,  a.session_start_ts_localtime,   a.session_end_ts_localtime,     a.hdp_create_ts,        a.hdp_update_ts,
current_timestamp as Az_insert_ts, current_timestamp as Az_update_ts, 0 as exec_run_id,a.event_date
from iptv.ltv_stream_fact as a 
where 
event_date >= '${hiveconf:DeltaPartStart}' and event_date < trunc(current_date, 'MM');

Insert overwrite  table  iptv.ltv_stream_fact_az Partition(event_date)
select a.ltv_sk, a.uuid, a.rogers_account_id,    a.comcast_account_id,   default.tokenize(a.rogers_device_id, 'K10') as rogers_device_id ,     a.comcast_device_id,    a.comcast_session_id,   a.session_start_ts_utc, a.session_end_ts_utc,   a.app_name,     a.app_version_num,      default.tokenize(a.ipaddress_num, 'K10') as ipaddress_num,        a.isp_name,     a.network_location_name,        a.user_agent_name,      a.asset_id,     a.asset_class_cd,       a.media_guid,   a.provider_id,  a.asset_content_id,     a.program_id,   a.merlin_recording_id,  a.merlin_stream_id,     a.compass_stream_id,    a.session_duration_count,       a.completion_status_cd, a.playback_type_cd,     a.playback_started_flag,        a.first_asset_position_num,     a.last_asset_position_num,      a.drm_latency_num,      a.open_latency_num,     a.trickplay_events_num, a.fragment_warning_events_num,  a.frame_rate_min_num,   a.frame_rate_max_num,   a.bit_rate_min_num,     a.bit_rate_max_num,     a.session_start_date_id,        a.session_start_time_id,        a.session_stop_date_id, a.session_stop_time_id, a.prg_station_id,       a.gn_program_id,        a.airing_start_date_id, a.airing_start_time_id, a.channel_sk,   a.subscriber_key,       a.subscriber_no,        a.subscriber_seq_no,    a.l9_subcriber_sub_type,        a.customer_key, a.customer_id,  a.address_key,  a.data_mac_id,  a.session_start_ts_localtime,   a.session_end_ts_localtime,     a.hdp_create_ts,        a.hdp_update_ts,
current_timestamp as Az_insert_ts, current_timestamp as Az_update_ts, 0 as exec_run_id,a.event_date
from iptv.ltv_stream_fact as a 
where 
event_date >=  date_sub('${hiveconf:DeltaPartStart}', 65) and event_date <  '${hiveconf:DeltaPartStart}' 
and hdp_create_ts >  cast(regexp_replace('${hiveconf:DeltahdpCreatTs}', 'T', ' ') as timestamp);

!echo ;
!echo Hive : Data loaded; 

