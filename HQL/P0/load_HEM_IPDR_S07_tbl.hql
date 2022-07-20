!echo ;
!echo Hive : Dropping External Table HEM.IPDR_S07_TEMP;
DROP TABLE IF EXISTS HEM.IPDR_S07_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.IPDR_S07_TEMP at /userapps/hem/landing/IPDR_S07;

CREATE EXTERNAL TABLE HEM.IPDR_S07_TEMP (
  CMTS_HOST_NAME          STRING,
  CMTS_SYS_UP_TIME        STRING,
  CMTS_MD_IF_NAME         STRING,
  CMTS_MD_IF_INDEX        STRING,
  CMTS_MD_CM_SG_ID        STRING,
  CMTS_RCP_ID             STRING,
  CMTS_RCC_STATUS_ID      STRING,
  CMTS_RCS_ID             STRING,
  CMTS_TCS_ID             STRING,
  CM_MAC_ADDR             STRING,
  CM_IPV4_ADDR            STRING,
  CM_IPV6_ADDR            STRING,
  CM_IPV6_LINK_LOCAL_ADDR STRING,
  CM_QOS_VERSION          STRING,
  CM_REG_STATUS_VALUE     STRING,
  CM_LAST_REG_TIME        STRING,
  REC_TYPE                STRING,
  REC_CREATION_TIME       STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/IPDR_S07'
tblproperties ("skip.header.line.count"="1");

!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.IPDR_S07;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE HEM.IPDR_S07 PARTITION (PROCESSED_TS)
SELECT
   CMTS_HOST_NAME,
   CAST(CMTS_SYS_UP_TIME AS BIGINT),
   CMTS_MD_IF_NAME,
   CAST(CMTS_MD_IF_INDEX AS BIGINT),
   CAST(CMTS_MD_CM_SG_ID AS BIGINT),
   CMTS_RCP_ID,
   CAST(CMTS_RCC_STATUS_ID AS BIGINT),
   CAST(CMTS_RCS_ID AS INT),
   CAST(CMTS_TCS_ID AS INT),
   CM_MAC_ADDR,
   CM_IPV4_ADDR,
   CM_IPV6_ADDR,
   CM_IPV6_LINK_LOCAL_ADDR,
   CAST(CM_QOS_VERSION AS INT),
   CAST(CM_REG_STATUS_VALUE AS INT),
   CAST(CM_LAST_REG_TIME as BIGINT),
   CAST(REC_TYPE AS INT),
   CAST(REC_CREATION_TIME as BIGINT),
   regexp_extract(INPUT__FILE__NAME,'(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])',0),
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp()),
   INPUT__FILE__NAME,
   '${hiveconf:JobExecutionID}',
   cast(from_unixtime(CAST('${hiveconf:RunTimestamp}' as BIGINT), 'yyyy-MM-dd HH:mm:ss') as timestamp) as PROCESSED_TS
FROM HEM.IPDR_S07_TEMP;

!echo ;
!echo Hive : Data loaded into HEM.IPDR_S07;

