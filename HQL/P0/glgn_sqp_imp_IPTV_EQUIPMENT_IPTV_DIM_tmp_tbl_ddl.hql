CREATE TABLE EXT_IPTV.EQUIPMENT_IPTV_DIM_SQP_IMP
(
customer_id       varchar(32), 
data_mac_id       varchar(180),
serial_number     varchar(30), 
sku               varchar(16),
equipment_type    string,
equipment_make    string,
equipment_model   string ,
app_name          varchar(100),
app_version       varchar(50),
ipaddress         varchar(50),
isp_name          varchar(100),
network_location  varchar(50),
user_agent        varchar(200),
device_type       string,
last_viewed_dt    timestamp,
hdp_create_ts     timestamp,
hdp_update_ts     timestamp 
)
STORED AS RCFILE;
