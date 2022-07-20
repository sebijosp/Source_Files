drop table data_pods.conv_web_acct_event_dly purge;
CREATE TABLE data_pods.conv_web_acct_event_dly(
  ecid bigint, 
  ban bigint, 
  hashed_account_number varchar(255), 
  brand string, 
  session_date date, 
  session_id varchar(255), 
  session_date_time timestamp, 
  webpage varchar(255), 
  webpage_tr string, 
  dynamic_content varchar(255), 
  join_key varchar(510),
  hdp_update_ts timestamp)
PARTITIONED BY (
  run_date date)
tblproperties ("orc.compress"="SNAPPY");
