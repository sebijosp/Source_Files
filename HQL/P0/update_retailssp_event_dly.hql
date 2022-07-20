ALTER TABLE data_pods.POD_RETAILSSP_CTN_DLY ADD columns (hdp_update_ts timestamp) CASCADE;

alter table data_pods.POD_RETAILSSP_CTN_DLY rename to data_pods.POD_RETAILSSP_CTN_DLY_bkp; 		--backkup

drop table data_pods.POD_RETAILSSP_CTN_DLY purge;
CREATE TABLE data_pods.POD_RETAILSSP_CTN_DLY(
ban varchar(12),
  ctn varchar(50),
  trans_type string,
  trans_event varchar(50),
  trans_status varchar(20),
  trans_date date,
  trans_time timestamp,
  transaction_direction string,
  dealer_code varchar(5),
  transaction_id varchar(10),
  grouped_transactions string,
  store_code varchar(10),
  chain_code varchar(10),
  brsc_id decimal(18,0),
  franchise varchar(1),
  location_name varchar(140),
  chain_name varchar(140),
  region varchar(50),
  city varchar(100),
  province varchar(12),
  channel_type_desc varchar(60),
  location_type varchar(510),
  hdp_update_ts timestamp)
PARTITIONED BY (
  run_date date)
tblproperties ("orc.compress"="SNAPPY");

--copy data
Insert overwrite table data_pods.POD_RETAILSSP_CTN_DLY partition(run_date)
select 
ban,
ctn,
trans_type,
trans_event,
trans_status,
trans_date,
trans_time,
transaction_direction,
dealer_code,
transaction_id,
grouped_transactions,
store_code,
chain_code,
brsc_id,
franchise,
location_name,
chain_name,
region,
city,
province,
channel_type_desc,
location_type,
hdp_update_ts,
trans_date
from data_pods.POD_RETAILSSP_CTN_DLY_bkp; 

--drop bkp table
drop table data_pods.POD_RETAILSSP_CTN_DLY_bkp purge;

alter table data_pods.POD_RETAILSSP_CTN_DLY rename to data_pods.WL_RETAILSSP_SUB_EVENT_DLY;
ALTER TABLE data_pods.WL_RETAILSSP_SUB_EVENT_DLY CHANGE CTN subscriber_no varchar(50);

--describe  table
desc data_pods.WL_RETAILSSP_SUB_EVENT_DLY;

