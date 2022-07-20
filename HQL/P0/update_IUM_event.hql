alter table data_pods.IUM_CATEGORY_LOOKUP rename to data_pods.DIM_WL_IUM_SUB_DLY;
alter table data_pods.wl_ium_sub_event_hrly rename to data_pods.wl_ium_sub_event_hrly_bkp; 		--backkup

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

--copy data
Insert overwrite table data_pods.wl_ium_sub_event_hrly partition(run_date)
select ban,         
subscriber_no,         
brand,       
segment_desc,
idv_ind,     
evp_ind,     
trans_type,
trans_dt,
notif_category,        
session_id,  
notification_id,        
notif_scenario_id,    
ium_75_ind,  
ium_90_ind,  
ium_100_ind, 
ium_suspend_ind,
trans_dt
from data_pods.wl_ium_sub_event_hrly_bkp; 

--drop bkp table
drop table data_pods.wl_ium_sub_event_hrly_bkp purge;

--describe  table
desc data_pods.wl_ium_sub_event_hrly;

