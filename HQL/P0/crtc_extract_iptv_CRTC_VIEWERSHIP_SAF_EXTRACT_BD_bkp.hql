
set hive.execution.engine=tez;
set mapreduce.task.timeout=360000000;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers=none;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

!echo Parms to Hive Script: START_DATE=${hiveconf:START_DATE}  END_DATE=${hiveconf:END_DATE} ;

!echo dropping table if exists :: CRTC_VIEWERSHIP.IPTV_SAF_EXTRACT_STG;

drop table if exists CRTC_VIEWERSHIP.IPTV_SAF_EXTRACT_STG;

!echo Creating table if not exists :: CRTC_VIEWERSHIP.IPTV_SAF_EXTRACT_STG;

CREATE TABLE if not exists CRTC_VIEWERSHIP.IPTV_SAF_EXTRACT_STG
(
CAN  VARCHAR(50),
PACKAGE VARCHAR(50),
TOTALBOXES VARCHAR(25),
SPEC_IND VARCHAR(500),
FSA_CODE VARCHAR(3),
DEVICE_ID VARCHAR(40)
)
STORED AS ORC;

!echo loading load on table :: CRTC_VIEWERSHIP.IPTV_SAF_EXTRACT_STG; 
insert into CRTC_VIEWERSHIP.IPTV_SAF_EXTRACT_STG
SELECT SERIAL_NO,PACKAGE,SETUP_BOX,SPEC_IND,FSA_CODE,DEVICE_ID FROM
(
SELECT t2.SERIAL_NO,t2.PACKAGE_PRIORTY,rank() over (partition by t2.SERIAL_NO order by (t2.PACKAGE_PRIORTY) asc) as rank,t2.PACKAGE,t2.SETUP_BOX,t2.SPEC_IND,t2.FSA_CODE,t2.DEVICE_ID
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
  t1.FSA_CODE,
  t1.DEVICE_ID
FROM
(
select C.SERIAL_NO as SERIAL_NO,
  TRIM(E.prod_desc) as PACKAGE_DESC,  -- PACKAGE_DESC
  B.CUSTOMER_TYPE as SPEC_IND,
  substr(LTRIM(A.SRVC_PSTL_CD),1,3) as FSA_CODE,
  max(D.settop_boxes) over (partition by A.CUSTOMER_KEY)  AS SETUP_BOX,
  D.DEVICE_ID as DEVICE_ID
from app_maestro.pntrdly A join
app_maestro.custdim B on A.CUSTOMER_KEY=B.CUSTOMER_KEY
join APP_MAESTRO.SBSCRBRDIM C on B.CUSTOMER_KEY=C.CUSTOMER_KEY AND B.CONTACT_KEY=C.CONTACT_KEY
join (select distinct CUSTOMER_ID, 1 as settop_boxes, 
    case when (length(trim(DATA_MAC_ID))= 12 or  length(trim(DATA_MAC_ID))= 24) then 
        concat(substring(trim(DATA_MAC_ID), 1, 2), ':', substring(trim(DATA_MAC_ID), 3, 2), ':', substring(trim(DATA_MAC_ID), 5, 2), ':', substring(trim(DATA_MAC_ID), 7, 2), ':', substring(trim(DATA_MAC_ID), 9, 2), ':', substring(trim(DATA_MAC_ID), 11, 2))
    else
        DATA_MAC_ID
    end as DEVICE_ID 
from ELA_OMS.X_TBEQUIPMENTTRACKER where EQUIPMENT_STATUS='A' AND DATA_MAC_ID IS NOT NULL AND SKU LIKE 'IPTV%')D
on D.CUSTOMER_ID=B.CUSTOMER_ID
join app_maestro.proddim E on E.PROD_ID=A.prod_ref_id
WHERE
B.CRNT_F='Y' AND B.MAESTRO_IND='Y' AND
C.CRNT_F='Y' AND C.MAESTRO_IND='Y' AND C.SUBSCRIBER_TYPE ='IP' AND c.SUB_STATUS='Active' AND 
to_date(CALENDAR_DATE)='2022-02-07' AND
-- to_date(CALENDAR_DATE)='${hiveconf:END_DATE}' AND
-- to_date(CALENDAR_DATE)= date_sub('${hiveconf:END_DATE}',1) AND
E.LOB_DESC='IPTV' and E.CRNT_F='Y' and E.lev_3_desc='Package - TSU' and E.BRAND='Rogers'
)t1
)t2
GROUP BY t2.SERIAL_NO,t2.PACKAGE_PRIORTY,t2.PACKAGE,t2.FSA_CODE,t2.SETUP_BOX,t2.SPEC_IND,t2.DEVICE_ID)t3
where t3.rank=1;


!echo loading iptv data on table :: CRTC_VIEWERSHIP.SAF_EXTRACT_STG;

Insert INTO TABLE CRTC_VIEWERSHIP.SAF_EXTRACT_STG
SELECT CAN,PACKAGE,TOTALBOXES,SPEC_IND,FSA_CODE,DEVICE_ID,'IPTV' as PRODUCT_SOURCE FROM
(
SELECT CAN,PACKAGE,rank() over (partition by CAN order by (SPEC_IND) asc) as spec_removal,rank() over (partition by CAN order by (FSA_CODE) asc) as fsa_removal,TOTALBOXES,SPEC_IND,FSA_CODE,DEVICE_ID
FROM
CRTC_VIEWERSHIP.IPTV_SAF_EXTRACT_STG)DUP
where DUP.fsa_removal=1 and DUP.spec_removal=1;

!echo dropping table if exists :: CRTC_VIEWERSHIP.SAF_EXTRACT;

drop table if exists CRTC_VIEWERSHIP.SAF_EXTRACT;

!echo Creating table the CRTC_VIEWERSHIP.SAF_EXTRACT table which will contain Both Legacy_Cable & IPTV Customers : CRTC_VIEWERSHIP.SAF_EXTRACT;

CREATE TABLE if not exists CRTC_VIEWERSHIP.SAF_EXTRACT
(
CAN  VARCHAR(50),
PACKAGE VARCHAR(50),
TOTALBOXES VARCHAR(25),
SPEC_IND VARCHAR(500),
FSA_CODE VARCHAR(3),
DEVICE_ID VARCHAR(40)
)
STORED AS ORC;

!echo loading final table with FSA_CODE more than 100 on table :: CRTC_VIEWERSHIP.SAF_EXTRACT;

Insert INTO TABLE CRTC_VIEWERSHIP.SAF_EXTRACT
SELECT
CAN,
PACKAGE,
TOTALBOXES,
SPEC_IND,
FSA_CODE,
DEVICE_ID
FROM
CRTC_VIEWERSHIP.SAF_EXTRACT_STG
where FSA_CODE IN (SELECT FSA_CODE FROM CRTC_VIEWERSHIP.SAF_EXTRACT_STG group by FSA_CODE  having count(*)>100);

!echo dropping table if exists :: CRTC_VIEWERSHIP.SAF_EXTRACT_CORUS;

drop table if exists CRTC_VIEWERSHIP.SAF_EXTRACT_CORUS;

!echo Creating table the CRTC_VIEWERSHIP.SAF_EXTRACT_CORUS table which will contain only IPTV Customers:: CRTC_VIEWERSHIP.SAF_EXTRACT_CORUS;

CREATE TABLE if not exists CRTC_VIEWERSHIP.SAF_EXTRACT_CORUS
(
CAN  VARCHAR(50),
PACKAGE VARCHAR(50),
TOTALBOXES VARCHAR(25),
SPEC_IND VARCHAR(500),
FSA_CODE VARCHAR(3),
DEVICE_ID VARCHAR(40)
)
STORED AS ORC;


Insert INTO TABLE CRTC_VIEWERSHIP.SAF_EXTRACT_CORUS
SELECT
CAN,
PACKAGE,
TOTALBOXES,
SPEC_IND,
FSA_CODE,
DEVICE_ID
FROM
CRTC_VIEWERSHIP.SAF_EXTRACT_STG
where FSA_CODE IN (SELECT FSA_CODE FROM CRTC_VIEWERSHIP.SAF_EXTRACT_STG group by FSA_CODE having count(*)>100)
and PRODUCT_SOURCE='IPTV';
