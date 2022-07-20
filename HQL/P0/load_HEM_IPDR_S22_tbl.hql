!echo ;
!echo Hive : Dropping External Table HEM.IPDR_S22_TEMP;
DROP TABLE IF EXISTS HEM.IPDR_S22_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.IPDR_S22_TEMP at /userapps/hem/landing/ipdr_S22;

CREATE EXTERNAL TABLE HEM.IPDR_S22_TEMP (
    CMTS_HOST_NAME STRING,
    CM_MAC_ADDR STRING,
    CMTS_DS_IF_INDEX BIGINT,
    Profile_Id BIGINT,
    Partial_Chan_Reason_Code INT,
    Last_Partial_Chan_Time BIGINT,
    Last_Partial_Chan_Reason_Code INT,
    Rec_Type INT,
    Rec_Creation_Time BIGINT
    )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/IPDR_S22'
tblproperties ("skip.header.line.count"="1");


!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.IPDR_S22_TEMP;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE HEM.IPDR_S22 PARTITION(PROCESSED_TS)
SELECT
    CMTS_HOST_NAME,
    CM_MAC_ADDR,
    CMTS_DS_IF_INDEX,
    Profile_Id,
    Partial_Chan_Reason_Code,
    Last_Partial_Chan_Time,
    Last_Partial_Chan_Reason_Code,
    Rec_Type,
    Rec_Creation_Time,
   regexp_extract(INPUT__FILE__NAME,'(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])',0) as cmts_ip_address,
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp()),
   INPUT__FILE__NAME,
   '${hiveconf:JobExecutionID}',
cast(from_unixtime(CAST('${hiveconf:RunTimestamp}' as BIGINT), 'yyyy-MM-dd HH:mm:ss') as timestamp) as PROCESSED_TS
FROM HEM.IPDR_S22_TEMP;

!echo ;
!echo Hive : Data loaded into HEM.IPDR_S22;

