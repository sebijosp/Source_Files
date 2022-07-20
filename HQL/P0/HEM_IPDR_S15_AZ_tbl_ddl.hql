CREATE TABLE hem.ipdr_s15_az(                                    
  cmts_host_name string COMMENT 'Fully Qualified Domain Name (FQDN) of the CMTS. If the CMTS does not have a domain name it contains an empty string.',
    cmts_sys_up_time bigint COMMENT '32-bit count of hundredths of a second since system initialization, in decimal notation.',
    cmts_md_if_index bigint COMMENT 'Interface Index for the CMTS MAC domain interface (described in CmtsMdIfName).',
    ds_if_index bigint COMMENT 'The Interface Index from the Interfaces Group MIB for the CMTS downstream interface.',
    ds_if_name string COMMENT 'First 50 characters of the Interface Name from the Interfaces Group MIB corresponding to the CMTS downstream interface.',
    ds_chid                  INT COMMENT 'DownStream channel id as defined in docsIf3MdChCfgChId.',
    ds_util_interval         INT COMMENT 'Time interval over which the channel utilization index is calculated. (sec)',
    ds_util_index_percentage INT COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
    ds_util_total_bytes bigint COMMENT 'No. of bytes in the payload portion of MPEG Packets(excludes MPEG header or pointer_field) transported by the DS interface.',
    ds_util_used_bytes bigint COMMENT 'No. DOCSIS data bytes transported by the DS interface. (Number of bytes - minus Number of stuff bytes) transported in DOCSIS payloads.',
    rec_type INT COMMENT '1 - Interim (Service Flow is still running). 2- Stop (Completed)  3-Start   4- Event (Single message record containing all the information)',
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

