DROP TABLE IF EXISTS HEM.NRM_RHSI_TOPOLOGY;
DROP TABLE IF EXISTS HEM.RHSI_TOPOLOGY_STG1;
DROP TABLE IF EXISTS HEM.RHSI_TOPOLOGY_DIM;

--FINAL TABLE WHERE FILE IS LOADED DAILY IN APPEND MODE
CREATE TABLE IF NOT EXISTS HEM.NRM_RHSI_TOPOLOGY(
   SAM_KEY                   STRING    COMMENT   'From NRM source',
   CMTS                      STRING    COMMENT   'From NRM source',
   PHUB                      STRING    COMMENT   'From NRM source',
   RTN_SEG                   STRING    COMMENT   'From NRM source',
   FWD_SEG                   STRING    COMMENT   'From NRM source',
   MAC_DOMAIN                STRING    COMMENT   'From NRM source',
   PORT_GROUP                STRING    COMMENT   'From NRM source',
   PORT_NAME                 STRING    COMMENT   'From NRM source',
   PORT_TYPE                 STRING    COMMENT   'From NRM source',
   PRODUCT_CD                STRING     COMMENT   'From NRM source',

   --ETL CONTROL COLUMNS BELOW
   MD5_HASH                  STRING     COMMENT   'From NRM source',
   HDP_INSERT_TS             TIMESTAMP  COMMENT   'Timestamp when the record was inserted into Hive',
   HDP_FILE_NAME             STRING     COMMENT   'Source file name from which the record was extracted',
   JOB_EXECUTION_ID          STRING     COMMENT   'Job Execution ID maps to execution ID of loadcontrol'
)
PARTITIONED BY (RECEIVED_DATE DATE)
STORED AS ORC;


--STEP 1 OF DIMENSION CREATION
CREATE TABLE IF NOT EXISTS HEM.RHSI_TOPOLOGY_STG1(
   SAM_KEY                   STRING       COMMENT   'From NRM source HEM.NRM_RHSI_TOPOLOGY',
   CBU                       STRING      COMMENT   'From reference table CBU_PHUB_REF',
   PHUB                      STRING       COMMENT   'From NRM source HEM.NRM_RHSI_TOPOLOGY',
   CMTS                      STRING       COMMENT   'From NRM source HEM.NRM_RHSI_TOPOLOGY',
   RTN_SEG                   STRING       COMMENT   'From NRM source HEM.NRM_RHSI_TOPOLOGY',
   FWD_SEG                   STRING       COMMENT   'From NRM source HEM.NRM_RHSI_TOPOLOGY',
   MAC_DOMAIN                STRING       COMMENT   'From NRM source HEM.NRM_RHSI_TOPOLOGY',
   MAC_ADDR                  STRING       COMMENT   'Mac address of modems from SS and Maestro tables based on sam_key',

   --ETL CONTROL COLUMNS BELOW
   CHANGE_TYPE               STRING       COMMENT   'I U X Flag for scd processiog',
   MD5_HASH                  STRING       COMMENT   'HASH Values for SCD procession',
   CRNT_FLG                  STRING       COMMENT   'Control values N/Y to indicate if latest record in chain of history',
   SRC_EFFECTIVE_DATE        TIMESTAMP   COMMENT   'Start date of record in system',
   SRC_END_DATE              TIMESTAMP   COMMENT   'End date of record in system',
   HDP_INSERT_TS             TIMESTAMP    COMMENT   'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS             TIMESTAMP    COMMENT   'Timestamp when the record was updated in Hive',
   JOB_EXECUTION_ID          STRING       COMMENT   'Job Execution ID maps to execution ID of loadcontrol'
)
STORED AS ORC;

--DAILY FILE LOAD
CREATE TABLE IF NOT EXISTS HEM.RHSI_TOPOLOGY_DIM(
   SAM_KEY                   STRING       COMMENT   'From NRM source HEM.NRM_RHSI_TOPOLOGY',
   CBU                       STRING      COMMENT   'From reference table CBU_PHUB_REF',
   PHUB                      STRING       COMMENT   'From NRM source HEM.NRM_RHSI_TOPOLOGY',
   CMTS                      STRING       COMMENT   'From NRM source HEM.NRM_RHSI_TOPOLOGY',
   RTN_SEG                   STRING       COMMENT   'From NRM source HEM.NRM_RHSI_TOPOLOGY',
   FWD_SEG                   STRING       COMMENT   'From NRM source HEM.NRM_RHSI_TOPOLOGY',
   MAC_DOMAIN                STRING       COMMENT   'From NRM source HEM.NRM_RHSI_TOPOLOGY',
   MAC_ADDR                  STRING       COMMENT   'Mac address of modems from SS and Maestro tables based on sam_key',

   --ETL CONTROL COLUMNS BELOW
   CHANGE_TYPE               STRING       COMMENT   'I U X Flag for scd processiog',
   MD5_HASH                  STRING       COMMENT   'HASH Values for SCD procession',
   CRNT_FLG                  STRING       COMMENT   'Control values N/Y to indicate if latest record in chain of history',
   SRC_EFFECTIVE_DATE        TIMESTAMP   COMMENT   'Start date of record in system',
   SRC_END_DATE              TIMESTAMP   COMMENT   'End date of record in system',
   HDP_INSERT_TS             TIMESTAMP    COMMENT   'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS             TIMESTAMP    COMMENT   'Timestamp when the record was updated in Hive',
   JOB_EXECUTION_ID          STRING       COMMENT   'Job Execution ID maps to execution ID of loadcontrol'
)
STORED AS ORC;
