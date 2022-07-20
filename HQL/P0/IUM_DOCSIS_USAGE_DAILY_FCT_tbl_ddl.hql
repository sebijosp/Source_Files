DROP TABLE HEM.IUM_DOCSIS_USAGE_DAILY_FCT;
CREATE TABLE HEM.IUM_DOCSIS_USAGE_DAILY_FCT(      
 --  DATE_KEY                 DATE      COMMENT 'DATE Key',
   CABLE_MODEM_KEY          BIGINT    COMMENT 'Cable Modem Key',
   NETWORK_TOPOLOGY_KEY     BIGINT    COMMENT 'Network Topology Key',
   CM_MAC_ADDR              STRING    COMMENT   'RHP Modem Mac Address',
   CM_IP_ADDRESS               STRING    COMMENT   '',
   CMTS_IP_ADDRESS               STRING    COMMENT   '',
   CMTS_HOST_NAME           STRING    COMMENT   'CMTS Host Name',
   CMTS_MD_IF_INDEX         BIGINT    COMMENT   'Contains the ifIndex for the CMTS MAC domain interface (described in CmtsMdIfName).',
   CMTS_MD_IF_NAME          STRING    COMMENT   'Contains the first 50 characters of the ifName from the Interfaces Group MIB for the row entry corresponding to the CMTS Mac Domain interface (ifType = 127).',
   STATE                    STRING    COMMENT   '',
   SERVICE_APP_ID           BIGINT    COMMENT   '',
   SERVICE_DS_MULTICAST     INT       COMMENT   '',
   SERVICE_CLASS   STRING    COMMENT   '',
   US_BYTES_AVG             decimal(30,4)    COMMENT   '',
   DS_BYTES_AVG             decimal(30,4)    COMMENT   '',
   USAGE_BYTES_AVG          decimal(30,4),
   US_BYTES                 decimal(30,4)    COMMENT   '',
   DS_BYTES                 decimal(30,4)    COMMENT   '',
   USAGE_BYTES              decimal(30,4),
   NBR_OF_HOURS             SMALLINT,
   HDP_INSERT_TS            TIMESTAMP COMMENT 'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS            TIMESTAMP COMMENT 'Timestamp when the record was updated in Hive',
   JOB_EXECUTION_ID         STRING    COMMENT 'Job Execution ID maps to execution ID of loadcontrol'
)
PARTITIONED BY (DATE_KEY DATE)
STORED AS ORC;
