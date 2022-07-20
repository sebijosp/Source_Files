CREATE TABLE iptv.ltv_str_stg_temp(
uuid                    string, 
rogers_account_id       string,
comcast_account_id      string, 
rogers_device_id        string, 
comcast_device_id       string,
comcast_session_id      string, 
session_start_ts_utc    timestamp,
session_end_ts_utc      timestamp,
app_name                string, 
app_version_num         string,
ipaddress_num           string,
isp_name                string, 
network_location_name   string,
user_agent_name         string,
asset_id                string, 
asset_class_cd          string, 
media_guid              string, 
provider_id             string,
asset_content_id        string, 
program_id              string, 
merlin_recording_id     string,
merlin_stream_id        string, 
compass_stream_id       bigint,
session_duration_count  bigint, 
completion_status_cd    string, 
playback_type_cd        string, 
playback_started_flag   char(1), 
first_asset_position_num bigint, 
last_asset_position_num  bigint,
drm_latency_num         bigint,
open_latency_num        bigint, 
trickplay_events_num    string, 
fragment_warning_events_num string,
frame_rate_min_num      bigint, 
frame_rate_max_num      bigint, 
bit_rate_min_num        bigint, 
bit_rate_max_num        bigint, 
hdp_create_ts           timestamp, 
hdp_update_ts           timestamp, 
channel_sk              bigint, 
subscriber_key          bigint, 
subscriber_no           string,
subscriber_seq_no       string,
l9_subcriber_sub_type   string,
customer_key            bigint, 
customer_id             string,
address_key             bigint,
data_mac_id             string,
error_code              string, 
first_create_dt         date,
session_start_date_id   bigint,
session_start_time_id   bigint,
session_start_ts_localtime timestamp,
session_end_ts_localtime timestamp)
STORED AS ORC;

