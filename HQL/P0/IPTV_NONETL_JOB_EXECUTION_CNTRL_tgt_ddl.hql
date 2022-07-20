CREATE TABLE IPTV.NONETL_JOB_EXECUTION_CNTRL
(
JOB_ID                    DECIMAL(15,0),
JOB_NAME                  VARCHAR(50),
EXECUTION_START_TS        TIMESTAMP,
EXECUTION_FINISH_TS       TIMESTAMP,
EXECUTION_STATUS          VARCHAR(50),
EXECUTION_ERROR_MSG       VARCHAR(500),
IPTV_JOB_NAME             VARCHAR(50),
IPTV_START_EXECUTION_ID   DECIMAL(15,0) ,
IPTV_END_EXECUTION_ID     DECIMAL(15,0) ,
IPTV_START_PREV_MAX_VALUE VARCHAR(50),
IPTV_END_CURR_MAX_VALUE   VARCHAR(50),
LAST_RUN_DATE             TIMESTAMP,
ETL_INSRT_DT              TIMESTAMP,
ETL_UPDT_DT               TIMESTAMP,
HDP_CREATE_TS             TIMESTAMP,
HDP_UPDATE_TS             TIMESTAMP
)STORED AS ORC
;
