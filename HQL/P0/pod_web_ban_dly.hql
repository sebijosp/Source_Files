drop table data_pods.pod_web_ban_dly;
CREATE TABLE data_pods.pod_web_ban_dly(
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
  join_key varchar(510))
PARTITIONED BY ( 
  calendar_year int, 
  calendar_month int);
