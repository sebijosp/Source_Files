create table data_pods.CONV_WEB_ACCT_EVENT_DLY_BKP2 as select * from data_pods.POD_WEB_BAN_DLY where session_date between '2021-09-01' and '2021-10-31';

ALTER TABLE data_pods.CONV_WEB_ACCT_EVENT_DLY_BKP2 ADD columns (hdp_update_ts timestamp);

Insert overwrite table data_pods.CONV_WEB_ACCT_EVENT_DLY partition(run_date)
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
from data_pods.CONV_WEB_ACCT_EVENT_DLY_BKP2;

drop table data_pods.CONV_WEB_ACCT_EVENT_DLY_BKP2 purge;
