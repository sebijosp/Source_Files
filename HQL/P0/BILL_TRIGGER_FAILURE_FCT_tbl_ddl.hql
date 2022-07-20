CREATE TABLE iptv.bill_trigger_failure_fct (
    mac_address                 varchar(255),
    provisioned_date             string,
    Discovery_date               string,
    Last_triggered_date          string,
    hdp_file_name                string,
    hdp_create_ts                timestamp,
    hdp_update_ts                timestamp,
    received_date                date   
  ) 
STORED AS ORC;
