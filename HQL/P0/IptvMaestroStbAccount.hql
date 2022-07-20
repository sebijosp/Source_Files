set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;


DROP TABLE IF EXISTS iptv.IPTV_MAESTRO_STB_AC;
CREATE TABLE iptv.IPTV_MAESTRO_STB_AC
AS SELECT 
DISTINCT 
D1.SUBSCRIBER_NO, 
D1.PRIM_RESOURCE_VAL,
D1.SUBSCRIBER_TYPE,
CASE WHEN D4.X_MESSAGE IS NULL OR D3.X_MESSAGE IS NULL THEN 'Y' ELSE 'N' END AS INCLUDE_FLG,
CASE WHEN D4.X_MESSAGE IS NOT NULL THEN 'Y' ELSE 'N' END AS EXCLUDE_FLG
FROM 
   ELA_ABP.SUBSCRIBER D1
     LEFT OUTER JOIN 
     APP_MAESTRO.CUSTDIM D2
                    ON D1.CUSTOMER_ID = D2.CUSTOMER_ID  
            --AND D2.CRNT_F='Y'
     LEFT OUTER JOIN APP_MAESTRO.ADDRDIM D3
                    ON D1.L9_SAM_KEY = D3.X_ADDRESS_ID
                    --AND D3.CRNT_F='Y'
     LEFT OUTER JOIN IPTV.MAESTRO_STB_X_MSG_REF D4
     ON D3.X_MESSAGE=D4.X_MESSAGE
WHERE 
--D1.CRNT_F='Y' AND 
NOT D1.SUB_STATUS in ('T','C')   
AND D1.SUBSCRIBER_TYPE='IP'
;
