create table iptv.VOD_RENTAL_SUMFCT (
rogers_account_id	VARCHAR(100),
comcast_account_id	VARCHAR(20),
rogers_device_id	VARCHAR(100),
comcast_device_id	VARCHAR(100),
comcast_session_id	VARCHAR(100),
app_name	VARCHAR(100),
app_version_num	VARCHAR(50),
ipaddress_num	VARCHAR(50),
isp_name	VARCHAR(100),
network_location_name	VARCHAR(50),
user_agent_name	VARCHAR(200),
program_id	VARCHAR(100),
media_guid	VARCHAR(200),
provider_id	VARCHAR(100),
asset_id	VARCHAR(20),
lease_info_start_of_window	TIMESTAMP,
lease_info_end_of_Window	TIMESTAMP,
lease_info_purchase_time	TIMESTAMP,
lease_info_lease_length	DECIMAL(19,0),
lease_info_lease_Id	VARCHAR(25),
lease_info_lease_price	DECIMAL(10,2),
rental_start_ts	TIMESTAMP,
rental_end_ts	TIMESTAMP,
rental_date	DATE,
content_duration	varchar(20),
rental_duration		int,
vod_category_cd	VARCHAR(20),
hdp_create_ts	TIMESTAMP,
hdp_update_ts	TIMESTAMP)
PARTITIONED BY (event_date	DATE)

STORED AS ORC

