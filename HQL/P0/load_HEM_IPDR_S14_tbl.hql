!echo ;
!echo Hive : Dropping External Table HEM.IPDR_S14_TEMP;
DROP TABLE IF EXISTS HEM.IPDR_S14_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table HEM.IPDR_S14_TEMP at /userapps/hem/landing/IPDR_S14;

CREATE EXTERNAL TABLE HEM.IPDR_S14_TEMP (
  cmtshostname           	     string                
,cmtssysuptime           	     bigint                 
,cmtsmdifindex          	     bigint                  
,usifindex              	     bigint
,usifname               	     string
,uschid                 	     int
,usutilinterval          	     int
,usutilindexpercentage   	     int
,usutiltotalmslots       	     bigint
,usutilucastgrantedmslots            bigint
,usutiltotalcntnmslots   	     bigint
,usutilusedcntnmslots   	     bigint
,usutilcollcntnmslots   	     bigint
,usutiltotalcntnreqmslots            bigint
,usutilusedcntnreqmslots 	     bigint
,usutilcollcntnreqmslots 	     bigint
,usutiltotalcntnreqdatamslots        bigint
,usutilusedcntnreqdatamslots         bigint
,usutilcollcntnreqdatamslots         bigint
,usutiltotalcntninitmaintmslots      bigint
,usutilusedcntninitmaintmslots       bigint
,usutilcollcntninitmaintmslots       bigint
,rectype                             int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/hem/landing/IPDR_S14'
tblproperties ("skip.header.line.count"="1");

!echo ;
!echo Hive : Temp Table created;
!echo Hive : Loading data into table HEM.IPDR_S14;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE HEM.IPDR_S14 PARTITION (PROCESSED_TS)
SELECT
 cmtshostname           	    
,cmtssysuptime           	    
,cmtsmdifindex          	    
,usifindex              	    
,usifname               	    
,uschid                 	    
,usutilinterval          	    
,usutilindexpercentage   	    
,usutiltotalmslots       	    
,usutilucastgrantedmslots       
,usutiltotalcntnmslots   	    
,usutilusedcntnmslots   	    
,usutilcollcntnmslots   	    
,usutiltotalcntnreqmslots       
,usutilusedcntnreqmslots 	    
,usutilcollcntnreqmslots 	    
,usutiltotalcntnreqdatamslots   
,usutilusedcntnreqdatamslots    
,usutilcollcntnreqdatamslots    
,usutiltotalcntninitmaintmslots 
,usutilusedcntninitmaintmslots  
,usutilcollcntninitmaintmslots  
,rectype 
,regexp_extract(INPUT__FILE__NAME,'(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])',0)
,from_unixtime(unix_timestamp())
,from_unixtime(unix_timestamp())
,INPUT__FILE__NAME
,'${hiveconf:JobExecutionID}'
,cast(from_unixtime(CAST('${hiveconf:RunTimestamp}' as BIGINT), 'yyyy-MM-dd HH:mm:ss') as timestamp) as PROCESSED_TS
FROM HEM.IPDR_S14_TEMP;

!echo ;
!echo Hive : Data loaded into HEM.IPDR_S14;



