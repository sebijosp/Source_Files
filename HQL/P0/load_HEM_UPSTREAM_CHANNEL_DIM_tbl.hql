!echo ;
!echo Hive : Dropping External Table HEM.UPSTREAM_CHANNEL_DIM_STG;
DROP TABLE IF EXISTS HEM.UPSTREAM_CHANNEL_DIM_STG;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.UPSTREAM_CHANNEL_DIM_STG at /userapps/hem/landing/UPSTREAM_CHANNEL_DIM;

CREATE EXTERNAL TABLE HEM.UPSTREAM_CHANNEL_DIM_STG (
   CMTS_NAME             STRING,
   MAC_DOMAIN_NAME       STRING,
   MAC_DOMAIN_TYPE       STRING,    
   PORT_GROUP            STRING,
   NODE                  STRING,
   CHANNEL_MODULATION    STRING,
   CHANNEL_NAME          STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/UPSTREAM_CHANNEL_DIM'
tblproperties ("skip.header.line.count"="1");

!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.UPSTREAM_CHANNEL_DIM;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE HEM.UPSTREAM_CHANNEL_DIM PARTITION (RECEIVED_DATE)
SELECT
   CMTS_NAME,
   MAC_DOMAIN_NAME,
   MAC_DOMAIN_TYPE,
   PORT_GROUP,  
   NODE,       
   CHANNEL_MODULATION,
   CHANNEL_NAME,
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp()),
   reverse(split(reverse(INPUT__FILE__NAME),"/")[0]),
   '${hiveconf:JobExecutionID}',
   '${hiveconf:ProcessedDate}'  as received_date
FROM HEM.UPSTREAM_CHANNEL_DIM_STG;

!echo ;
!echo Hive : Data loaded into HEM.UPSTREAM_CHANNEL_DIM;
