DROP TABLE IF EXISTS HEM.IPDR_S22;
CREATE TABLE HEM.IPDR_S22(
    CMTS_HOST_NAME STRING,
    CM_MAC_ADDR STRING,
    CMTS_DS_IF_INDEX BIGINT,
    Profile_Id BIGINT,
    Partial_Chan_Reason_Code BIGINT,
    Last_Partial_Chan_Time BIGINT,
    Last_Partial_Chan_Reason_Code BIGINT,
    Rec_Type BIGINT,
    Rec_Creation_Time BIGINT,
   CMTS_IP_ADDRESS           STRING     COMMENT   'IP Address off CMTS derived from the file name',
   HDP_INSERT_TS             TIMESTAMP  COMMENT   'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS             TIMESTAMP  COMMENT   'Timestamp when the record was updated in Hive',
   HDP_FILE_NAME             STRING     COMMENT   'Source file name from which the record was extracted',
   JOB_EXECUTION_ID          STRING     COMMENT   'Job Execution ID maps to execution ID of loadcontrol'
)
PARTITIONED BY (PROCESSED_TS TIMESTAMP)
STORED AS ORC;
