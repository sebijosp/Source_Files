create table IPTV.CHROMECAST_MONTHLY_USAGE(
service_account_id      string,
account_source_id    string,
device_name  string,
hdp_insert_ts  timestamp,
hdp_update_ts  timestamp
)PARTITIONED BY(event_month string)
stored as ORC;
