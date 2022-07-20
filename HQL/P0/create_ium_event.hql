drop table data_pods.wl_ium_sub_event_hrly purge;
CREATE TABLE data_pods.wl_ium_sub_event_hrly(
ban bigint,
subscriber_no bigint,
brand varchar(1),
segment_desc varchar(80),
idv_ind smallint,
evp_ind smallint,
trans_type varchar(50),
trans_dt date,
notif_category varchar(39),
session_id varchar(32767),
notification_id  varchar(32767),
notif_scenario_id  varchar(32767),
ium_75_ind smallint,
ium_90_ind smallint,
ium_100_ind smallint,
ium_suspend_ind smallint)

PARTITIONED BY (
run_date date)
tblproperties ("orc.compress"="SNAPPY");
