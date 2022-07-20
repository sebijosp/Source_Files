ALTER TABLE data_pods.POD_WEB_BAN_DLY ADD columns (hdp_update_ts timestamp) CASCADE;

alter table data_pods.POD_WEB_BAN_DLY rename to data_pods.POD_WEB_BAN_DLY_bkp; 		--backkup

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
  join_key varchar(510),
  hdp_update_ts timestamp)
PARTITIONED BY (
  run_date date)
tblproperties ("orc.compress"="SNAPPY");
  
--copy data
Insert overwrite table data_pods.POD_WEB_BAN_DLY partition(run_date)
select 
ecid , 
  ban , 
  hashed_account_number , 
  brand , 
  session_date , 
  session_id , 
  session_date_time , 
  webpage , 
  webpage_tr , 
  dynamic_content , 
  join_key ,
  hdp_update_ts ,
  session_date
from data_pods.POD_WEB_BAN_DLY_bkp; 

--drop bkp table
--drop table data_pods.POD_WEB_BAN_DLY_bkp purge;

alter table data_pods.POD_WEB_BAN_DLY rename to data_pods.CONV_WEB_ACCT_EVENT_DLY;

--describe  table
desc data_pods.CONV_WEB_ACCT_EVENT_DLY;

