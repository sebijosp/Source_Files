set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

!echo inserting data into table iptv.ignite_tickets_trucks  for delta startdate : ${hiveconf:start_date} and enddate : ${hiveconf:end_date}  ;


CREATE OR REPLACE VIEW iptv.vw_techops_trucks
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
      ,t1.X_SCHEDULING_TEXT
     ,t1.X_SPECIAL_INSTRUCTIONS
      ,t1.X_TECH_EN_ROUTE
      ,t1.X_TECH_ON_SITE
      ,t1.X_TECH_COMPLETE
      ,t1.X_TECH_COMMENTS
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
  LEFT JOIN table_case_ref t4 ON t1.X_WORK_ORDER_ID = t4.X_SERVICE_ORDER_ID
  Where T1.X_TECH_COMPLETE between '${hiveconf:start_date}' and '${hiveconf:end_date}'
  and T1.X_TECH_COMPLETE is not null
  and t1.X_WORK_ORDER_ID is not null
  --where  T1.X_TECH_COMPLETE between date_sub(current_date(),13330) and current_date()
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
,tickets.CBP 
,current_timestamp() as hdp_insert_ts
,current_timestamp() as hdp_update_ts
,date_format(t2.X_TECH_COMPLETE_TS ,'yyyy-MM') as tech_complete_month
from comcast_daily t2
LEFT JOIN ODS_REMEDY.COMMON_TICKET tickets on t2.ticket_number = tickets.txt_originatingticketid
LEFT JOIN billing_hhid on t2.X_CUSTOMER_ID = billing_hhid.CUSTOMER_ID
;

!echo vw_techops_trucks created successfully;

 with merged_data as (select X_CREATE_DATE
,X_WORK_ORDER_ID
,X_TECH_COMPLETE_TS
,X_CUSTOMER_ID
,X_SCHEDULING_SYSTEM_ID
,X_RESCHEDULE_COUNT
,X_SCHEDULING_TEXT
,X_SPECIAL_INSTRUCTIONS
,X_TECH_EN_ROUTE
,X_TECH_ON_SITE
,X_TECH_COMPLETE
,X_TECH_COMMENTS
,WOM_STATUS_TITLE
,WO_STATUS
,PRODUCT_LOB
,MAP_AREA_CODE
,MANAGEMENT_AREA_CODE
,FRANCHISE_AREA_CD
,CBU_CODE
,DWELLING_TYPE_DESC
,SMT
,PHUB
,NODE
,CBP  ACCOUNT_ID
,TICKET_ID
,SUBSCRIPTION_ID
,SERVICE_TYPE
,Create_date
,TICKET_CLOSE_DATETIME
,MODIFIED_DATE
,CASE_LEVEL_1
,CASE_LEVEL_2
,CASE_LEVEL_3
,CASE_LEVEL_4
,CASE_LEVEL_5
,TICKET_STATUS
,hdp_insert_ts
,hdp_update_ts
,tech_complete_month, 1 as sort_order from iptv.vw_techops_trucks delta
 UNION ALL select tgt.*, 2 as sort_order from iptv.ignite_tickets_trucks tgt
 where tech_complete_month in (select distinct tech_complete_month  from iptv.vw_techops_trucks ))

insert overwrite table iptv.ignite_tickets_trucks  partition (tech_complete_month)
 select X_CREATE_DATE
      ,X_WORK_ORDER_ID
      ,X_TECH_COMPLETE_TS
      ,X_CUSTOMER_ID
      ,X_SCHEDULING_SYSTEM_ID
      ,X_RESCHEDULE_COUNT
      ,X_SCHEDULING_TEXT
      ,X_SPECIAL_INSTRUCTIONS
      ,X_TECH_EN_ROUTE
      ,X_TECH_ON_SITE
      ,X_TECH_COMPLETE
      ,X_TECH_COMMENTS
      ,WOM_STATUS_TITLE
      ,WO_STATUS
      ,PRODUCT_LOB
      ,MAP_AREA_CODE
      ,MANAGEMENT_AREA_CODE
      ,FRANCHISE_AREA_CD
      ,CBU_CODE
      ,DWELLING_TYPE_DESC
      ,SMT
      ,PHUB
      ,NODE
      ,ACCOUNT_ID
      ,TICKET_ID
   ,SUBSCRIPTION_ID
  ,SERVICE_TYPE
,Create_date
,TICKET_CLOSE_DATETIME
,MODIFIED_DATE
,CASE_LEVEL_1
,CASE_LEVEL_2
,CASE_LEVEL_3
,CASE_LEVEL_4
,CASE_LEVEL_5
,TICKET_STATUS
,hdp_insert_ts
,hdp_update_ts
,tech_complete_month
 from (select *, row_number() over ( Partition by x_work_order_id,X_TECH_COMPLETE_TS order by sort_order asc) rank_order from merged_data ) merged_ranked
 where merged_ranked.rank_order = 1 
;
!echo data loaded successfully;
