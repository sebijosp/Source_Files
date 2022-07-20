!echo ;
!echo Hive : Dropping External Table HEM.IPDR_S10_TEMP;
DROP TABLE IF EXISTS HEM.IPDR_S10_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.IPDR_S10_TEMP at /userapps/hem/landing/IPDR_S10;

CREATE EXTERNAL TABLE HEM.IPDR_S10_TEMP (
   CMTS_HOST_NAME                           STRING,
   CMTS_SYS_UP_TIME                         STRING,
   CMTS_MD_IF_NAME                          STRING,
   CMTS_MD_IF_INDEX                         STRING,
   CM_MAC_ADDR                              STRING,
   CM_REG_STATUS_ID                         STRING,
   CMTS_CM_US_CH_IF_NAME                    STRING,
   CMTS_CM_US_CH_IF_INDEX                   STRING,
   CMTS_CM_US_CH_ID                         INT,
   CMTS_CM_US_MODULATION_TYPE               INT,
   CMTS_CM_US_RX_POWER                      INT,
   CMTS_CM_US_SIGNAL_NOISE                  INT,
   CMTS_CM_US_MICROREFLECTIONS              INT,
   CMTS_CM_US_EQ_DATA                       STRING,
   CMTS_CM_US_UNERROREDS                    STRING,
   CMTS_CM_US_CORRECTEDS                    INT,
   CMTS_CM_US_UNCORRECTABLES                INT,
   CMTS_CM_US_HIGH_RESOLUTION_TIMING_OFFSET STRING,
   CMTS_CM_US_IS_MUTED                      BOOLEAN,
   CMTS_CM_US_RANGING_STATUS                INT,
   REC_TYPE                                 INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/IPDR_S10'
tblproperties ("skip.header.line.count"="1");


!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.IPDR_S10_TEMP;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE HEM.IPDR_S10 PARTITION(PROCESSED_TS)
SELECT
   CMTS_HOST_NAME,
   CAST(CMTS_SYS_UP_TIME AS BIGINT),
   CMTS_MD_IF_NAME,
   CAST(CMTS_MD_IF_INDEX AS BIGINT),
   CM_MAC_ADDR,
   CAST(CM_REG_STATUS_ID AS BIGINT),  
   CMTS_CM_US_CH_IF_NAME,
   CAST(CMTS_CM_US_CH_IF_INDEX AS BIGINT),
   CMTS_CM_US_CH_ID,
   CMTS_CM_US_MODULATION_TYPE,
   CMTS_CM_US_RX_POWER,
   CMTS_CM_US_SIGNAL_NOISE,
   CMTS_CM_US_MICROREFLECTIONS,
   CMTS_CM_US_EQ_DATA,
   CAST(CMTS_CM_US_UNERROREDS AS BIGINT),
   CMTS_CM_US_CORRECTEDS,
   CMTS_CM_US_UNCORRECTABLES,
   CMTS_CM_US_HIGH_RESOLUTION_TIMING_OFFSET,
   CMTS_CM_US_IS_MUTED,
   CMTS_CM_US_RANGING_STATUS,
   CAST(REC_TYPE AS INT),
   regexp_extract(INPUT__FILE__NAME,'(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])',0),
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp()),
   INPUT__FILE__NAME,
   '${hiveconf:JobExecutionID}',
   cast(from_unixtime(CAST('${hiveconf:RunTimestamp}' as BIGINT), 'yyyy-MM-dd HH:mm:ss') as timestamp) as PROCESSED_TS
FROM HEM.IPDR_S10_TEMP;

!echo ;
!echo Hive : Data loaded into HEM.IPDR_S10;
