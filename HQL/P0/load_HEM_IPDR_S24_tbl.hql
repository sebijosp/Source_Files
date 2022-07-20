!echo ;
!echo Hive : Dropping External Table HEM.IPDR_S24_TEMP;
DROP TABLE IF EXISTS HEM.IPDR_S24_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.IPDR_S24_TEMP at /userapps/hem/landing/ipdr_S24;

CREATE EXTERNAL TABLE HEM.IPDR_S24_TEMP (
    CMTS_HOST_NAME	STRING,
    CMTS_DS_IF_INDEX	STRING,
    Profile_Id	STRING,
    FullChannelSpeed	STRING,
    OutOctets	STRING,
    OutUnicastOctets	STRING,
    OutMulticastOctets	STRING,
    OutFrames	STRING,
    OutUnicastFrames	STRING,
    OutMulticastFrames	STRING,
    CounterDiscontinuityTime	STRING,
    RecType	STRING,
    RecCreationTime	STRING
    )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/IPDR_S24'
tblproperties ("skip.header.line.count"="1");


!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.IPDR_S24_TEMP;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE HEM.IPDR_S24 PARTITION(PROCESSED_TS)
SELECT
   CMTS_HOST_NAME,
   CMTS_DS_IF_INDEX,
   Profile_Id,
   FullChannelSpeed,
   OutOctets,
   OutUnicastOctets,
   OutMulticastOctets,
   OutFrames,
   OutUnicastFrames,
   OutMulticastFrames,
   CounterDiscontinuityTime,
   RecType,
   RecCreationTime,
   regexp_extract(INPUT__FILE__NAME,'(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])',0) as cmts_ip_address,
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp()),
   INPUT__FILE__NAME,
   '${hiveconf:JobExecutionID}',
cast(from_unixtime(CAST('${hiveconf:RunTimestamp}' as BIGINT), 'yyyy-MM-dd HH:mm:ss') as timestamp) as PROCESSED_TS
FROM HEM.IPDR_S24_TEMP;

!echo ;
!echo Hive : Data loaded into HEM.IPDR_S24;

