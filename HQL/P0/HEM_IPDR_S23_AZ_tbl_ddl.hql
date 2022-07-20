CREATE TABLE hem.ipdr_s23_az(                                    
  cmts_host_name string,
    cm_mac_addr string,
    cmts_ds_if_index bigint,
    cmlastregtime bigint,
    curpartialsvcreasoncode bigint,
    lastpartialsvctime bigint COMMENT 'This attribute returns the date and time when the MAC indicated that this CM recovered from its most recent Partial Service incident on this downstream OFDM/upstream OFDMA channel',
    lastpartialsvcreasoncode bigint COMMENT 'This attribute returns the last CM-STATUS Event Code which indicates the reason that this CM was experiencing Partial Service on this downstream OFDM/upstream OFDMA channel. (Note: if the CM is currently experiencing Partial Service, this is the Event Code',
    numpartialsvcincidents bigint COMMENT 'This attribute returns the number of Partial Service incidents the MAC layer has reported for this CM on this downstream OFDM/upstream OFDMA channel',
    numpartialchanincidents bigint COMMENT 'This attributes returns the number of Partial Channel incidents the MAC layer has reported for this CM on this downstream OFDM/upstream OFDMA channel',
    rectype INT COMMENT 'Contains the IPDR record type. Interim identifies a running record. Stop identifies the end of a record. Start identifies the start of a record. Event identifies a single message record containing all information.',
    reccreationtime bigint COMMENT 'Contains a 64-bit count of milliseconds UTC time stamp at the time the data for the record was acquired',
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

