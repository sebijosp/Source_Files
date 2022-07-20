!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.IUM_MAC_USAGE_FCT;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE HEM.IUM_MAC_USAGE_FCT PARTITION (EVENT_DATE)
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
   hdp_file_name,
   to_date(from_unixtime(unix_timestamp(substring(Start_Time,0,10),'MM/dd/yyyy')))as EVENT_DATE
FROM HEM.IUM_MAC_USAGE_RAW
WHERE RECEIVED_DATE >'${hiveconf:DeltaPartStart}';

!echo ;
!echo Hive : Data loaded into HEM.IUM_MAC_USAGE_FCT;
