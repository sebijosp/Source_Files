!echo ;
!echo Hive : Dropping External Table HEM.IPDR_S15_TEMP;
DROP TABLE IF EXISTS HEM.IPDR_S15_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.IPDR_S15_TEMP at /userapps/hem/landing/IPDR_S15;

CREATE EXTERNAL TABLE HEM.IPDR_S15_TEMP (
Cmts_Host_Name				STRING		
,Cmts_Sys_Up_Time			BIGINT		
,Cmts_Md_If_Index			BIGINT		
,Ds_If_Index				BIGINT		
,Ds_If_Name				STRING		
,Ds_ChId				INT			
,Ds_Util_Interval			INT			
,Ds_Util_Index_Percentage 		INT			
,Ds_Util_Total_Bytes			BIGINT		
,Ds_Util_Used_Bytes			BIGINT		
,Rec_Type				INT 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/IPDR_S15'
tblproperties ("skip.header.line.count"="1");

!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.IPDR_S15;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE HEM.IPDR_S15 PARTITION (PROCESSED_TS)
SELECT
 Cmts_Host_Name	
,Cmts_Sys_Up_Time	
,Cmts_Md_If_Index	
,Ds_If_Index			
,Ds_If_Name				
,Ds_ChId				
,Ds_Util_Interval
,Ds_Util_Index_Percentage
,Ds_Util_Total_Bytes	
,Ds_Util_Used_Bytes		
,Rec_Type
,regexp_extract(INPUT__FILE__NAME,'(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])',0)
,from_unixtime(unix_timestamp())
,from_unixtime(unix_timestamp())
,INPUT__FILE__NAME
,'${hiveconf:JobExecutionID}'
,cast(from_unixtime(CAST('${hiveconf:RunTimestamp}' as BIGINT), 'yyyy-MM-dd HH:mm:ss') as timestamp) as PROCESSED_TS
FROM HEM.IPDR_S15_TEMP;

!echo ;
!echo Hive : Data loaded into HEM.IPDR_S15;

