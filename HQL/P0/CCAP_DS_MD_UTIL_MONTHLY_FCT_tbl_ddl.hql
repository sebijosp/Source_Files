DROP TABLE HEM.CCAP_DS_MD_UTIL_MONTHLY_FCT; 
CREATE TABLE HEM.CCAP_DS_MD_UTIL_MONTHLY_FCT
(
   month_id                  decimal(4,0)   COMMENT 'month Id',
   Network_Topology_Key     BIGINT    comment 'Network Topology Key',
   Cmts_Host_Name           STRING    COMMENT 'Fully Qualified Domain Name (FQDN) of the CMTS. If the CMTS does not have a domain name it contains an empty string.',
   Cmts_Md_If_Index         BIGINT    COMMENT 'Interface Index for the CMTS MAC domain interface (described in CmtsMdIfName).',
   Ds_Util_Interval         INT       COMMENT 'Time interval over which the channel utilization index is calculated.(sec)',
   Interface_Count          BIGINT    comment 'Count of the Interface',
   Is_md_lvl_flg            STRING    COMMENT  'Flag indicates S-SCQAM,O-OFDMA , P=Port',
   Ds_Util_Total_Bytes      BIGINT    COMMENT 'No. of bytes in the payload portion of MPEG Packets(excludes MPEG header or pointer_field) transported by the DS interface.',
   Ds_Util_Used_Bytes       BIGINT    COMMENT 'No. DOCSIS data bytes transported by the DS interface. (Number of bytes - minus Number of stuff bytes) transported in DOCSIS payloads.',
   capacity_bps             DOUBLE    COMMENT 'Capacity in bytes per second',
   ds_throughput_bps_max    DOUBLE    COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
   ds_throughput_bps_95     DOUBLE    COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
   ds_throughput_bps_98     DOUBLE    COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
   Ds_Util_Index_Percentage_max decimal(10,2)       COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
   Ds_Util_Index_Percentage_95 decimal(10,2)       COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
   Ds_Util_Index_Percentage_98 decimal(10,2)       COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
   rop                      BIGINT    COMMENT 'No of times port congested in a day ',
   ds_util_index_percentage    decimal(10,2)       COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.', 
   Time_flag                 STRING    COMMENT 'is_pt for P and is_full_day for F',
   HDP_INSERT_TS            TIMESTAMP COMMENT 'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS            TIMESTAMP COMMENT 'Timestamp when the record was updated in Hive',
   JOB_EXECUTION_ID         STRING    COMMENT 'Job Execution ID maps to execution ID of loadcontrol'
) PARTITIONED BY (Date_Key date)
STORED AS ORC;
