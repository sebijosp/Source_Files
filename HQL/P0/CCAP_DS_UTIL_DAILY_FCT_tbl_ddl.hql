DROP TABLE HEM.CCAP_DS_UTIL_DAILY_FCT;
CREATE TABLE HEM.CCAP_DS_UTIL_DAILY_FCT
(
   Network_Topology_Key     BIGINT    comment 'Network Topology Key',
   Interface_Key                        BIGINT    comment 'Topology Key',
   Cmts_Host_Name           STRING    COMMENT 'Fully Qualified Domain Name (FQDN) of the CMTS. If the CMTS does not have a domain name it contains an empty string.',
   Cmts_Md_If_Index         BIGINT    COMMENT 'Interface Index for the CMTS MAC domain interface (described in CmtsMdIfName).',
   Ds_If_Index              BIGINT    COMMENT 'The Interface Index from the Interfaces Group MIB for the CMTS downstream interface.',
   Ds_If_Name               STRING    COMMENT 'First 50 characters of the Interface Name from the Interfaces Group MIB corresponding to the CMTS downstream interface.',
   Ds_ChId                  INT       COMMENT 'DownStream channel id as defined in docsIf3MdChCfgChId.',
   Ds_Util_Interval         INT       COMMENT 'Time interval over which the channel utilization index is calculated.(sec)',
   Ds_Util_Total_Bytes      BIGINT    COMMENT 'No. of bytes in the payload portion of MPEG Packets(excludes MPEG header or pointer_field) transported by the DS interface.',
   Ds_Util_Used_Bytes       BIGINT    COMMENT 'No. DOCSIS data bytes transported by the DS interface. (Number of bytes - minus Number of stuff bytes) transported in DOCSIS payloads.',
   capacity_bps             DOUBLE    COMMENT 'Capacity in bytes per second',
   Ds_throughput_bps_max    INT       COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
   Ds_throughput_bps_95     INT       COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
   Ds_throughput_bps_98     INT       COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
   Ds_Util_Index_Percentage_max decimal(10,2)       COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
   Ds_Util_Index_Percentage_95 decimal(10,2)       COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
   Ds_Util_Index_Percentage_98 decimal(10,2)       COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
   HDP_INSERT_TS            TIMESTAMP COMMENT 'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS            TIMESTAMP COMMENT 'Timestamp when the record was updated in Hive',
   JOB_EXECUTION_ID         STRING    COMMENT 'Job Execution ID maps to execution ID of loadcontrol'

) PARTITIONED BY (DATE_KEY DATE)
STORED AS ORC;
