CREATE TABLE EXT_IPTV.LTV_CHANNEL_DIM_SQP_IMP
(channel_sk         decimal(18,0),
source_id          decimal(18,0),
gracenote_id       string,       
x1_station_id      string,       
call_sign          string,       
channel_name       string,       
provider_name      string,       
channel_format     string,       
mode_of_access_cd  string,       
device_type_cd     string,       
cdvr_enabled_flag  string,       
hdp_create_ts      timestamp,    
hdp_update_ts      timestamp,    
crnt_flg           string,       
src_eff_dt         timestamp,    
src_end_dt         timestamp
)
STORED AS RCFILE;
