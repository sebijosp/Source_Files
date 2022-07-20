CREATE TABLE IPTV.APP_EVENTS_SUMFCT(
header_ts                               bigint,
uuid                                    string,
hostname                                string,
receiver_id                             string,
device_id                               string,
devicesource_id                         string,
account                                 string,
account_source_id                       string,
billing_account_id                      string,
mac_address                             string,
ecm_mac_address                         string,
firmware_version                        string,
device_type                             string,
device_make                             string,
device_model                            string,
partner                                 string,
ip_address                              string,
utc_offset                              int,
postal_code                             string,
event_timestamp         bigint,            
app_id                  string,            
app_name                string,            
app_action              string,            
event_name              string,            
evt_src                 string,            
session_guid            string,            
event_type              string,            
event_description       string,            
page_name               string,            
button_name             string,            
error_code              string,            
hdp_file_name           string,            
hdp_create_ts           string,            
hdp_update_ts           string        
)
PARTITIONED BY (event_date      DATE)
STORED AS ORC;


