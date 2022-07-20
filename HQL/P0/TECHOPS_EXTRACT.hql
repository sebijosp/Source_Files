set hive.exec.compress.output=false;
set mapreduce.output.fileoutputformat.compress=false;
CREATE OR REPLACE VIEW iptv.TECHOPS_EXTRACT_VW
AS

WITH table_case_ref as
(
select X_SERVICE_ORDER_ID , id_number from (
Select  X_SERVICE_ORDER_ID ,id_number, row_number()
over (partition by X_SERVICE_ORDER_ID order by update_ts desc ) rnk from ods_crm.table_case )a
where a.rnk=1
),
 comcast_daily as
(
SELECT Distinct t1.X_CREATE_DATE
      ,t1.X_WORK_ORDER_ID
      ,t1.X_TECH_COMPLETE_TS
      ,t1.X_CUSTOMER_ID
      ,t1.X_SCHEDULING_SYSTEM_ID
      ,t1.X_RESCHEDULE_COUNT
      ,cast(null as string) as X_SCHEDULING_TEXT
      ,cast(null as string) as X_SPECIAL_INSTRUCTIONS
      ,t1.X_TECH_EN_ROUTE
      ,t1.X_TECH_ON_SITE
      ,t1.X_TECH_COMPLETE
      ,cast(null as string) as X_TECH_COMMENTS
      ,t1.WOM_STATUS_TITLE
      ,t2.WO_STATUS
      ,t2.PRODUCT_LOB
      ,t3.MAP_AREA_CODE
      ,t3.MANAGEMENT_AREA_CODE
      ,t3.FRANCHISE_AREA_CD
      ,t3.CBU_CODE
      ,t3.DWELLING_TYPE_DESC
      ,t3.SMT
      ,t3.PHUB
      ,t3.SHUB as NODE,
    t4.id_number ticket_number
  FROM MAESTROBI.VWTBLXWOMWOTRANS  T1  
  Left JOIN APP_MAESTRO.WODTLFCT  T2 on T1.X_SCHEDULING_SYSTEM_ID = T2.SCHEDULING_SYSTEM_ID 
  LEFT JOIN APP_MAESTRO.ADDRDIM  T3 on t2.ADDRESS_KEY=T3.ADDRESS_KEY 
  LEFT JOIN table_case_ref t4  ON t1.X_WORK_ORDER_ID = t4.X_SERVICE_ORDER_ID
  Where  T1.X_TECH_COMPLETE between '${hiveconf:start_date}' and '${hiveconf:end_date}'
  and T1.WORK_TYPE_CODE_REF_ID IN ('IGTV','DPSM','IGHS','IGRH','I1','I2','P1','P2') and T3.CRNT_F  = 'Y'
),
billing_hhid as (
Select customer_id, HHID from (
Select subscriber.customer_id,HHID , row_number() over (partition by subscriber.customer_id order by SYS_UPDATE_DATE desc ) rnk
from ods_abp.subscriber  , app_maestro.CUST_INTERNET_EQUIP_DIM
where subscriber.CRNT_F = 'Y' 
and  DATA_MAC_ID  = PRIM_RESOURCE_VAL 
and CUST_INTERNET_EQUIP_DIM.CRNT_FLG = 'Y' )a where a.rnk = 1
)
SELECT  
  t2.X_CREATE_DATE
      ,t2.X_WORK_ORDER_ID
      ,t2.X_TECH_COMPLETE_TS
      ,t2.X_CUSTOMER_ID
      ,t2.X_SCHEDULING_SYSTEM_ID
      ,t2.X_RESCHEDULE_COUNT
      ,t2.X_SCHEDULING_TEXT
      ,t2.X_SPECIAL_INSTRUCTIONS
      ,t2.X_TECH_EN_ROUTE
      ,t2.X_TECH_ON_SITE
      ,t2.X_TECH_COMPLETE
      ,t2.X_TECH_COMMENTS
      ,t2.WOM_STATUS_TITLE
      ,t2.WO_STATUS
      ,t2.PRODUCT_LOB
      ,t2.MAP_AREA_CODE
      ,t2.MANAGEMENT_AREA_CODE
      ,t2.FRANCHISE_AREA_CD
      ,t2.CBU_CODE
      ,t2.DWELLING_TYPE_DESC
      ,t2.SMT
      ,t2.PHUB
      ,t2.NODE 
        ,tickets.CBP  ACCOUNT_ID
         ,tickets.TICKET_ID
   ,billing_hhid.hhid SUBSCRIPTION_ID
  ,tickets.SERVICE_TYPE
,tickets.Create_date
,tickets.TICKET_CLOSE_DATETIME
,tickets.MODIFIED_DATE
,tickets.CASE_LEVEL_1
,tickets.CASE_LEVEL_2
,tickets.CASE_LEVEL_3
,tickets.CASE_LEVEL_4
,tickets.CASE_LEVEL_5
,tickets.TICKET_STATUS
,tickets.CBP from comcast_daily t2 
LEFT JOIN ODS_REMEDY.COMMON_TICKET tickets on t2.ticket_number = tickets.txt_originatingticketid
LEFT JOIN billing_hhid on t2.X_CUSTOMER_ID = billing_hhid.CUSTOMER_ID
;

INSERT OVERWRITE DIRECTORY '/apps/hive/warehouse/iptv.db/techops_extracts/${hiveconf:log_date}/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
NULL DEFINED AS ''
STORED AS TEXTFILE
select * from iptv.techops_extract_vw;

drop view  iptv.techops_extract_vw;

