!echo ;
!echo Hive : Dropping External Table HEM.NRM_RHSI_TOPOLOGY_STG;
DROP TABLE IF EXISTS HEM.NRM_RHSI_TOPOLOGY_STG;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.NRM_RHSI_TOPOLOGY_STG at /userapps/hem/landing/NRM_RHSI_TOPOLOGY_STG;

CREATE EXTERNAL TABLE HEM.NRM_RHSI_TOPOLOGY_STG (
   CMTS           STRING COMMENT   'From NRM file',
   MAC_DOMAIN     STRING COMMENT   'From NRM file',
   PORT_GROUP     STRING COMMENT   'From NRM file',
   PORT_NAME      STRING COMMENT   'From NRM file',
   PORT_TYPE      STRING COMMENT   'From NRM file',
   RTN_Seg        STRING COMMENT   'From NRM file',
   FWD_Seg        STRING COMMENT   'From NRM file',
   PHUB           STRING COMMENT   'From NRM file',
   SAMKEY         STRING COMMENT   'From NRM file',
   PRODUCT_CD     STRING COMMENT   'From NRM file'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/NRM_RHSI_TOPOLOGY_STG'
tblproperties ("skip.header.line.count"="1");

!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.NRM_RHSI_TOPOLOGY;

INSERT INTO TABLE HEM.NRM_RHSI_TOPOLOGY PARTITION (RECEIVED_DATE)
SELECT
   SAMKEY,
   CMTS,
   PHUB,
   RTN_SEG,
   FWD_SEG,
   concat('20000',lpad(regexp_replace(MAC_DOMAIN,'md',''),2,0)),
   PORT_GROUP,
   PORT_NAME,
   PORT_TYPE,
   PRODUCT_CD,
   null,
   from_unixtime(unix_timestamp()),
   INPUT__FILE__NAME,
   '${hiveconf:JobExecutionID}',
   cast(from_unixtime(CAST('${hiveconf:RunTimestamp}' as BIGINT), 'yyyy-MM-dd') as date) as RECEIVED_DATE
FROM HEM.NRM_RHSI_TOPOLOGY_STG;

!echo ;
!echo Hive : Data loaded into HEM.NRM_RHSI_TOPOLOGY;
