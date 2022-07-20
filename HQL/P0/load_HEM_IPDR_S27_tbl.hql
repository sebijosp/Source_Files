!echo ;
!echo Hive : Dropping External Table HEM.IPDR_S27_TEMP;
DROP TABLE IF EXISTS HEM.IPDR_S27_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.IPDR_S27_TEMP at /userapps/hem/landing/IPDR_S27;

CREATE EXTERNAL TABLE HEM.IPDR_S27_TEMP (
    CMTS_HOST_NAME	STRING	,
    CMTS_CM_US_CH_IF_INDEX	BIGINT	,
    DataIuc	INT	,
    TotalCodewords	BIGINT	,
    CorrectedCodewords	BIGINT	,
    UnreliableCodewords	BIGINT	,
    InOctets	BIGINT	,
    CounterDiscontinuityTime	BIGINT	,
    RecType	INT,
    RecCreationTime	BIGINT
    )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/IPDR_S27'
tblproperties ("skip.header.line.count"="1");


!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.IPDR_S27_TEMP;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE HEM.IPDR_S27 PARTITION(PROCESSED_TS)
SELECT
   CMTS_HOST_NAME,
   CMTS_CM_US_CH_IF_INDEX,
   DataIuc,
   TotalCodewords,
   CorrectedCodewords,
   UnreliableCodewords,
   InOctets,
   CounterDiscontinuityTime,
   RecType,
   RecCreationTime,
   regexp_extract(INPUT__FILE__NAME,'(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])',0) as cmts_ip_address,
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp()),
   INPUT__FILE__NAME,
   '${hiveconf:JobExecutionID}',
cast(from_unixtime(CAST('${hiveconf:RunTimestamp}' as BIGINT), 'yyyy-MM-dd HH:mm:ss') as timestamp) as PROCESSED_TS
FROM HEM.IPDR_S27_TEMP;

!echo ;
!echo Hive : Data loaded into HEM.IPDR_S27;

