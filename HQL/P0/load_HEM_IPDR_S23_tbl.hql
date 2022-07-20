!echo ;
!echo Hive : Dropping External Table HEM.IPDR_S23_TEMP;
DROP TABLE IF EXISTS HEM.IPDR_S23_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.IPDR_S23_TEMP at /userapps/hem/landing/ipdr_S23;

CREATE EXTERNAL TABLE HEM.IPDR_S23_TEMP (
   CMTS_HOST_NAME STRING,
   CM_MAC_ADDR STRING,
   CMTS_DS_IF_INDEX BIGINT,
   Cm_Last_Reg_Time BIGINT,
   Cur_Partial_Svc_Reason_Code BIGINT,
   Last_Partial_Svc_Time BIGINT,
   Last_Partial_Svc_Reason_Code BIGINT,
   Num_Partial_Svc_Incidents BIGINT,
   Num_Partial_Chan_Incidents BIGINT,
   Rec_Type BIGINT,
   Rec_Creation_Time BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/IPDR_S23'
tblproperties ("skip.header.line.count"="1");


!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.IPDR_S23_TEMP;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE HEM.IPDR_S23 PARTITION(PROCESSED_TS)
SELECT
   CMTS_HOST_NAME,
   CM_MAC_ADDR,
   CMTS_DS_IF_INDEX,
   CAST(Cm_Last_Reg_Time AS BIGINT),
   Cur_Partial_Svc_Reason_Code,
   Last_Partial_Svc_Time,
   Last_Partial_Svc_Reason_Code,
   Num_Partial_Svc_Incidents,
   Num_Partial_Chan_Incidents,
   Rec_Type,
   Rec_Creation_Time,
   regexp_extract(INPUT__FILE__NAME,'(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])',0),
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp()),
   INPUT__FILE__NAME,
   '${hiveconf:JobExecutionID}',
    cast(from_unixtime(CAST('${hiveconf:RunTimestamp}' as BIGINT), 'yyyy-MM-dd HH:mm:ss') as timestamp) as PROCESSED_TS
FROM HEM.IPDR_S23_TEMP;

!echo ;
!echo Hive : Data loaded into HEM.IPDR_S23;

