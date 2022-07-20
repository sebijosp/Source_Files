CREATE TABLE HEM.IPDR_S15(
Cmts_Host_Name           STRING    COMMENT 'Fully Qualified Domain Name (FQDN) of the CMTS. If the CMTS does not have a domain name it contains an empty string.',
Cmts_Sys_Up_Time         BIGINT    COMMENT '32-bit count of hundredths of a second since system initialization, in decimal notation.',   
Cmts_Md_If_Index         BIGINT    COMMENT 'Interface Index for the CMTS MAC domain interface (described in CmtsMdIfName).',
Ds_If_Index              BIGINT    COMMENT 'The Interface Index from the Interfaces Group MIB for the CMTS downstream interface.',
Ds_If_Name               STRING    COMMENT 'First 50 characters of the Interface Name from the Interfaces Group MIB corresponding to the CMTS downstream interface.',   
Ds_ChId                  INT       COMMENT 'DownStream channel id as defined in docsIf3MdChCfgChId.',
Ds_Util_Interval         INT       COMMENT 'Time interval over which the channel utilization index is calculated. (sec)',
Ds_Util_Index_Percentage INT       COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
Ds_Util_Total_Bytes      BIGINT    COMMENT 'No. of bytes in the payload portion of MPEG Packets(excludes MPEG header or pointer_field) transported by the DS interface.',
Ds_Util_Used_Bytes       BIGINT    COMMENT 'No. DOCSIS data bytes transported by the DS interface. (Number of bytes - minus Number of stuff bytes) transported in DOCSIS payloads.',
Rec_Type                 INT       COMMENT '1 - Interim (Service Flow is still running). 2- Stop (Completed)  3-Start   4- Event (Single message record containing all the information)',
CMTS_IP_ADDRESS          STRING    COMMENT 'IP Address off CMTS derived from the file name',
HDP_INSERT_TS            TIMESTAMP COMMENT 'Timestamp when the record was inserted into Hive',
HDP_UPDATE_TS            TIMESTAMP COMMENT 'Timestamp when the record was updated in Hive',
HDP_FILE_NAME            STRING    COMMENT 'Source file name from which the record was extracted',
JOB_EXECUTION_ID         STRING    COMMENT 'Job Execution ID maps to execution ID of loadcontrol'
)
PARTITIONED BY (PROCESSED_TS TIMESTAMP)
STORED AS ORC;

