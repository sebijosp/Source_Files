CREATE TABLE hem.ipdr_s22_az(                                    
    cmts_host_name string,
    cm_mac_addr string,
    cmts_ds_if_index bigint,
    profile_id bigint,
    partial_chan_reason_code bigint,
    last_partial_chan_time bigint,
    last_partial_chan_reason_code bigint,
    rec_type bigint,
    rec_creation_time bigint,
    cmts_ip_address string COMMENT 'IP Address off CMTS derived from the file name',
    hdp_insert_ts TIMESTAMP COMMENT 'Timestamp when the record was inserted into Hive',
    hdp_update_ts TIMESTAMP COMMENT 'Timestamp when the record was updated in Hive',
    hdp_file_name string COMMENT 'Source file name from which the record was extracted',
    job_execution_id string COMMENT 'Job Execution ID maps to execution ID of loadcontrol',
    az_insert_ts TIMESTAMP COMMENT 'ETL insert time to azure',
    az_update_ts TIMESTAMP COMMENT 'ETL update ts to azure',
    exec_run_id STRING COMMENT 'ETL exec run id ie pipeline runid'
)   
PARTITIONED BY (processed_ts timestamp)  
STORED AS ORC;  

