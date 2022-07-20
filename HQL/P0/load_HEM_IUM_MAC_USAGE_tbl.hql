!echo ;
!echo Hive : Dropping External Table HEM.IUM_MAC_USAGE_TEMP;
DROP TABLE IF EXISTS HEM.IUM_MAC_USAGE_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.IUM_MAC_USAGE_TEMP at /userapps/hem/landing/IUM_MAC_USAGE;

CREATE EXTERNAL TABLE HEM.IUM_MAC_USAGE_TEMP (
Start_Time                  STRING   ,
   End_Time                 STRING  ,
   IP_Address               STRING  ,
   CM_MAC_ADDR              STRING  ,
   STATE                    STRING  ,
   SCEIP                    STRING  ,
   UP_BYTES                 STRING   ,
   DOWN_BYTES               STRING   ,
   CMTS_HOST_NAME           STRING   ,
   CMTS_SYS_UPTIME          STRING   ,
   REC_TYPE                 STRING   ,
   REC_CREATION_TIME        STRING   ,
   SERVICE_IDENTIFIER       STRING   ,
   CMTS_IPV6_ADDR           STRING   ,
   CMTS_MD_IF_NAME          STRING   ,
   CM_IPV6_ADDR             STRING   ,
   CM_IPV6_LINK_LOCAL_ADDR  STRING   ,
   CM_QOS_VERSION           STRING   ,
   CM_REG_STATUS_VALUE      STRING   ,
   CM_LAST_REG_TIME         STRING   ,
   SERVICE_FLOW_CH_SET      STRING   ,
   SERVICE_APP_ID           STRING   ,
   SERVICE_DS_MULTICAST     STRING   ,
   SERVICE_GATE_ID          STRING   ,
   SERVICE_PKTS_PASSED      STRING   ,
   SERVICE_SLA_DROP_PKTS    STRING   ,
   SERVICE_SLA_DELAY_PKTS   STRING   ,
   SERVICE_TIME_CREATED     STRING   ,
   SERVICE_TIME_ACTIVE      STRING   ,
   CMTS_MD_IF_INDEX         STRING   ,
   TARGET_TYPE              STRING   ,
   REPORTED_SERVICE_CLASS   STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/IUM_MAC_USAGE'
tblproperties ("skip.header.line.count"="1");

!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.IUM_MAC_USAGE_RAW;

select * from HEM.IUM_MAC_USAGE_TEMP limit 3;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE HEM.IUM_MAC_USAGE_RAW PARTITION (RECEIVED_DATE)
SELECT
   Start_Time             ,
   End_Time               , 
   IP_Address             , 
   CM_MAC_ADDR            , 
   STATE                  , 
   SCEIP                  , 
   UP_BYTES               , 
   DOWN_BYTES             , 
   CMTS_HOST_NAME         , 
   CMTS_SYS_UPTIME        , 
   REC_TYPE               , 
   REC_CREATION_TIME      , 
   SERVICE_IDENTIFIER     , 
   CMTS_IPV6_ADDR         , 
   CMTS_MD_IF_NAME        , 
   CM_IPV6_ADDR           , 
   CM_IPV6_LINK_LOCAL_ADDR, 
   CM_QOS_VERSION         , 
   CM_REG_STATUS_VALUE    , 
   CM_LAST_REG_TIME       , 
   SERVICE_FLOW_CH_SET    , 
   SERVICE_APP_ID         , 
   SERVICE_DS_MULTICAST   , 
   SERVICE_GATE_ID        , 
   SERVICE_PKTS_PASSED    , 
   SERVICE_SLA_DROP_PKTS  , 
   SERVICE_SLA_DELAY_PKTS , 
   SERVICE_TIME_CREATED   , 
   SERVICE_TIME_ACTIVE    , 
   CMTS_MD_IF_INDEX       , 
   TARGET_TYPE            , 
   REPORTED_SERVICE_CLASS ,
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp()),
   INPUT__FILE__NAME,
   '${hiveconf:JobExecutionID}',
   '${hiveconf:ProcessedDate}'  as received_date
FROM HEM.IUM_MAC_USAGE_TEMP;

!echo ;
!echo Hive : Data loaded into HEM.IUM_MAC_USAGE_RAW;
