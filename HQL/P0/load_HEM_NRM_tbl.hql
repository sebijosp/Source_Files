!echo ;
!echo Hive : Dropping External Table HEM.NRM_TEMP;
DROP TABLE IF EXISTS HEM.NRM_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.NRM_TEMP at /userapps/hem/landing/nrm;
CREATE EXTERNAL TABLE HEM.NRM_TEMP (
KEY_CMTS_MDIFINDEX      STRING,
   KEY_CMTS_MDIFINDEX_PORT STRING,
   CMTS                    STRING,
   PORTGROUP               STRING,
   PORTNAME                STRING,
   DSPORT                  STRING,
   MDIFINDEX               INT,
   RTN_SEG                 STRING,
   FWD_SEG                 STRING,
   PHUB                    STRING,
   LOADDATE                TIMESTAMP,
   HDP_UPDATE_TS           TIMESTAMP,
   HDP_INSERT_TS           TIMESTAMP   
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/nrm'
tblproperties ("skip.header.line.count"="1");

!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.NRM;

INSERT OVERWRITE TABLE HEM.NRM 
SELECT
   KEY_CMTS_MDIFINDEX,
   KEY_CMTS_MDIFINDEX_PORT,
   CMTS,
   PORTGROUP,
   PORTNAME,
   DSPORT,
   MDIFINDEX,
   RTN_SEG,
   FWD_SEG,
   PHUB,
   LOADDATE,
   HDP_UPDATE_TS,
   HDP_INSERT_TS,
   INPUT__FILE__NAME,
   '${hiveconf:JobExecutionID}'
FROM HEM.NRM_TEMP;

!echo ;
!echo Hive : Data loaded into HEM.NRM_TEMP;

