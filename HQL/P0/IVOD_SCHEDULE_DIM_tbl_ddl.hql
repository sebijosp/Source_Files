create table iptv.ivod_schedule_dim	
(
ivod_id			varchar(100),
ivod_status_cd		varchar(10),
program_title_nm	varchar(300),
src_added_ts		timestamp,
media_guid		varchar(200),
region_id		varchar(400),
requested_id		varchar(200),
start_ts		timestamp,
program_desc_nm		varchar(400),
station_guid		int,
media_account_nm	varchar(30),
expiration_ts		timestamp,
prg_start_ts		timestamp,
hdp_create_ts 		timestamp,
hdp_update_ts 		timestamp
)
partitioned by (event_date Date)
STORED AS ORC;
