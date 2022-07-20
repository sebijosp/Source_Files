set hive.execution.engine=tez;
set mapreduce.task.timeout=360000000;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers=none;
set hive.optimize.sort.dynamic.partition=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

!echo Parms to Hive Script: START_DATE=${hiveconf:START_DATE}  END_DATE=${hiveconf:END_DATE} ;

!echo dropping table if exists :: SANDBOX.IPTV_SAF_EXTRACT_STG;

drop table if exists SANDBOX.IPTV_SAF_EXTRACT_STG;

!echo Creating table if not exists :: SANDBOX.IPTV_SAF_EXTRACT_STG;

CREATE TABLE if not exists SANDBOX.IPTV_SAF_EXTRACT_STG
(
CAN  VARCHAR(50),
PACKAGE VARCHAR(50),
TOTALBOXES VARCHAR(25),
SPEC_IND VARCHAR(500),
FSA_CODE VARCHAR(3)
)
STORED AS ORC;

!echo loading load on table :: SANDBOX.IPTV_SAF_EXTRACT_STG; 
insert into SANDBOX.IPTV_SAF_EXTRACT_STG
SELECT SERIAL_NO,PACKAGE,SETUP_BOX,SPEC_IND,FSA_CODE FROM
(
SELECT t2.SERIAL_NO,t2.PACKAGE_PRIORTY,rank() over (partition by t2.SERIAL_NO order by (t2.PACKAGE_PRIORTY) asc) as rank,t2.PACKAGE,t2.SETUP_BOX,t2.SPEC_IND,t2.FSA_CODE
  FROM
(  
SELECT 
  t1.SERIAL_NO,
  CASE
  WHEN t1.PACKAGE_DESC IN ('Select') THEN 1
  WHEN t1.PACKAGE_DESC IN ('Popular') THEN 2
  WHEN t1.PACKAGE_DESC IN ('VIP') THEN 3
  WHEN t1.PACKAGE_DESC IN ('Premier') THEN 4
  WHEN t1.PACKAGE_DESC IN ('Starter Tier') THEN 5
  ELSE 6 END AS PACKAGE_PRIORTY,
  t1.PACKAGE_DESC as PACKAGE,
  t1.SETUP_BOX,
  t1.SPEC_IND,
  t1.FSA_CODE  
FROM
(
select C.SERIAL_NO as SERIAL_NO,
  TRIM(E.prod_desc) as PACKAGE_DESC,  -- PACKAGE_DESC
  B.CUSTOMER_TYPE as SPEC_IND,
  substr(LTRIM(A.SRVC_PSTL_CD),1,3) as FSA_CODE,
max(D.settop_boxes) over (partition by A.CUSTOMER_KEY)  AS SETUP_BOX
from app_maestro.pntrdly A join
app_maestro.custdim B on A.CUSTOMER_KEY=B.CUSTOMER_KEY
join APP_MAESTRO.SBSCRBRDIM C on B.CUSTOMER_KEY=C.CUSTOMER_KEY AND B.CONTACT_KEY=C.CONTACT_KEY
join (select CUSTOMER_ID,count(distinct DATA_MAC_ID) as settop_boxes from ELA_OMS.X_TBEQUIPMENTTRACKER where EQUIPMENT_STATUS='A' AND DATA_MAC_ID IS NOT NULL group by CUSTOMER_ID)D
on D.CUSTOMER_ID=B.CUSTOMER_ID
join app_maestro.proddim E on E.PROD_ID=A.prod_ref_id
WHERE
B.CRNT_F='Y' AND B.MAESTRO_IND='Y' AND
C.CRNT_F='Y' AND C.MAESTRO_IND='Y' AND C.SUBSCRIBER_TYPE ='IP' AND c.SUB_STATUS='Active' AND 
to_date(CALENDAR_DATE)=date_sub(current_date,2) AND
-- to_date(CALENDAR_DATE)='${hiveconf:END_DATE}'AND
E.LOB_DESC='IPTV' and E.CRNT_F='Y' and E.lev_3_desc='Package - TSU' and E.BRAND='Rogers'
)t1
)t2
GROUP BY t2.SERIAL_NO,t2.PACKAGE_PRIORTY,t2.PACKAGE,t2.FSA_CODE,t2.SETUP_BOX,t2.SPEC_IND)t3
where t3.rank=1;


!echo loading iptv data on table :: SANDBOX.SAF_EXTRACT_STG;

Insert INTO TABLE SANDBOX.SAF_EXTRACT_STG
SELECT
CAN,
PACKAGE,
TOTALBOXES,
SPEC_IND,
FSA_CODE,
'IPTV'
FROM
SANDBOX.IPTV_SAF_EXTRACT_STG;


!echo dropping table if exists :: SANDBOX.SAF_EXTRACT;

drop table if exists SANDBOX.SAF_EXTRACT;

!echo Creating table if not exists :: SANDBOX.SAF_EXTRACT;

CREATE TABLE if not exists SANDBOX.SAF_EXTRACT
(
CAN  VARCHAR(50),
PACKAGE VARCHAR(50),
TOTALBOXES VARCHAR(25),
SPEC_IND VARCHAR(500),
FSA_CODE VARCHAR(3)
)
STORED AS ORC;


!echo loading final table with FSA_CODE more than 100 on table :: SANDBOX.SAF_EXTRACT;

Insert INTO TABLE SANDBOX.SAF_EXTRACT
SELECT
CAN,
PACKAGE,
TOTALBOXES,
SPEC_IND,
FSA_CODE
FROM
SANDBOX.SAF_EXTRACT_STG
where FSA_CODE IN (SELECT FSA_CODE FROM SANDBOX.SAF_EXTRACT_STG group by FSA_CODE having count(*)>100);

!echo iptv SAF has completed successfully;
