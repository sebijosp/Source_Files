!echo ;
!echo Hive : Dropping External Table HEM.NTD_TEMP;
DROP TABLE IF EXISTS HEM.NTD_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.NTD_TEMP at /userapps/hem/landing/ntd;
CREATE EXTERNAL TABLE HEM.NTD_TEMP (
   MAC             STRING,
   PHUB            STRING,
   CMTSIP          STRING,
   NODE            STRING,
   SMT             STRING,
   STREAM          STRING,
   FWDSEG          STRING,
   CLAMSHELL       STRING,
   STREET_NUMBER   STRING,
   STREET_NAME     STRING,
   STREET_TYPE     STRING,
   STREET_COMPASS  STRING,
   APT_NUMBER      STRING,
   POSTAL_CODE     STRING,
   CITY_NAME       STRING,
   PROVINCE        STRING,
   PRODUCT_CODE    STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/ntd'
tblproperties ("skip.header.line.count"="1");

!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.NTD;

INSERT OVERWRITE TABLE HEM.NTD 
SELECT
   MAC,
   PHUB,
   CMTSIP,
   NODE,
   SMT,
   STREAM,
   FWDSEG,
   CLAMSHELL,
   CAST(STREET_NUMBER AS DECIMAL),
   STREET_NAME,
   STREET_TYPE,
   STREET_COMPASS,
   APT_NUMBER,
   POSTAL_CODE,
   CITY_NAME,
   PROVINCE,
   PRODUCT_CODE,
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp()),
   INPUT__FILE__NAME,
  '${hiveconf:JobExecutionID}'
FROM HEM.NTD_TEMP;

!echo ;
!echo Hive : Data loaded into HEM.NTD_TEMP;

