create table IPTV.CHANNEL_DIM (
source_id	DECIMAL(18,0),
gracenote_id	VARCHAR(50),
x1_station_id	VARCHAR(100),
call_sign	VARCHAR(50),
channel_name	VARCHAR(50),
provider_name	VARCHAR(50),
channel_format	VARCHAR(50),
cdvr_enabled_flag	VARCHAR(50),
mode_of_access_cd	VARCHAR(50),
device_type_cd	VARCHAR(50),
hdp_create_ts	TIMESTAMP,
hdp_update_ts	TIMESTAMP )

stored as ORC
