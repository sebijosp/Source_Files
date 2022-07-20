CREATE TABLE IPTV.NONETL_IPTV_JOB_RELATN_REF (
JOB_NAME            VARCHAR(100),
JOB_TYPE            VARCHAR(100),
JOB_DOMAIN          VARCHAR(100),
JOB_STATUS          VARCHAR(100),
JOB_EXECUTION_RANGE VARCHAR(100),
IPTV_PIPELINE       VARCHAR(100),
IPTV_JOB_NAME       VARCHAR(100),
IPTV_SOURCE_TABLE   VARCHAR(100),
ETL_INSRT_DT         TIMESTAMP,
ETL_UPDT_DT          TIMESTAMP,
HDP_CREATE_TS        TIMESTAMP,
HDP_UPDATE_TS        TIMESTAMP
)STORED AS ORC
;
