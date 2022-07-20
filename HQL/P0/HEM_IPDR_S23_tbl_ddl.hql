DROP TABLE IF EXISTS HEM.IPDR_S23;
CREATE TABLE HEM.IPDR_S23(
   CMTS_HOST_NAME STRING,
   CM_MAC_ADDR STRING,
   CMTS_DS_IF_INDEX BIGINT,
   CmLastRegTime BIGINT,
   CurPartialSvcReasonCode BIGINT,
   LastPartialSvcTime BIGINT COMMENT 'This attribute returns the date and time when the MAC indicated that this CM recovered from its most recent Partial Service incident on this downstream OFDM/upstream OFDMA channel',
   LastPartialSvcReasonCode BIGINT COMMENT 'This attribute returns the last CM-STATUS Event Code which indicates the reason that this CM was experiencing Partial Service on this downstream OFDM/upstream OFDMA channel. (Note: if the CM is currently experiencing Partial Service, this is the Event Code from the previous Partial Service event.) A value of 0 indicates that the CM has not experienced Partial Service involving this OFDM/OFDMA channel during the CCAPs history of this CM',
   NumPartialSvcIncidents BIGINT COMMENT 'This attribute returns the number of Partial Service incidents the MAC layer has reported for this CM on this downstream OFDM/upstream OFDMA channel',
   NumPartialChanIncidents BIGINT COMMENT 'This attributes returns the number of Partial Channel incidents the MAC layer has reported for this CM on this downstream OFDM/upstream OFDMA channel',
   RecType INT COMMENT 'Contains the IPDR record type. Interim identifies a running record. Stop identifies the end of a record. Start identifies the start of a record. Event identifies a single message record containing all information.',
   RecCreationTime BIGINT COMMENT 'Contains a 64-bit count of milliseconds UTC time stamp at the time the data for the record was acquired',
    CMTS_IP_ADDRESS           STRING     COMMENT   'IP Address off CMTS derived from the file name',
    HDP_INSERT_TS             TIMESTAMP  COMMENT   'Timestamp when the record was inserted into Hive',
    HDP_UPDATE_TS             TIMESTAMP  COMMENT   'Timestamp when the record was updated in Hive',
    HDP_FILE_NAME             STRING     COMMENT   'Source file name from which the record was extracted',
    JOB_EXECUTION_ID          STRING     COMMENT   'Job Execution ID maps to execution ID of loadcontrol'
 )
PARTITIONED BY (PROCESSED_TS TIMESTAMP)
STORED AS ORC;
