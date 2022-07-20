CREATE TABLE iptv.dvr_schedule_dim
(
program_listing_id decimal(25,0),
ref_recording_id varchar(200),
program_title_nm varchar(300),
program_start_ts timestamp,
listing_id decimal(25,0),
station_id decimal(25,0),
hdp_create_ts TIMESTAMP,
hdp_update_ts timestamp
)
partitioned by (event_date Date)
STORED AS ORC;
