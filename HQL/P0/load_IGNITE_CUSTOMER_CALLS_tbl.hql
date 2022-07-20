set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

!echo inserting data into table iptv.ignite_customer_calls  for delta startdate : ${hiveconf:start_date} and enddate : ${hiveconf:end_date}  ;

create or replace view iptv.vw_techops_customer_calls
as
with customer_iptv_subscription as
(
Select customer_id , SUBSCRIPTION_ID from (
Select  customer_id, PRIM_RESOURCE_VAL SUBSCRIPTION_ID , row_number() over (partition by customer_id order by SYS_UPDATE_DATE desc ) rnk
 from ods_abp.subscriber where
 PRIM_RESOURCE_TP = 'ITV' and CRNT_F = 'Y' and PRIM_RES_PARAM_CD = 'W3ID'  )a where a.rnk = 1 )
Select  distinct
X_CONN_ID               CONN_ID,
TXT_ACCOUNTNUMBER       Acct_Number,
CASE_LEVEL_1            calltype1,
CASE_LEVEL_2            calltype2,
CASE_LEVEL_3            calltype3,
CASE_LEVEL_4            calltype4,
CASE_LEVEL_5            calltype5,
table_case.X_PROD_LINE  lineofbusiness,
START_TIME ,
END_TIME ,
(unix_timestamp(END_TIME) - unix_timestamp(START_TIME))/60 * 60 duration ,
customer_iptv_subscription.SUBSCRIPTION_ID SUBSCRIPTION_ID,
CURRENT_TIMESTAMP() AS HDP_INSERT_TS,
CURRENT_TIMESTAMP() AS HDP_UPDATE_TS,
date_format(START_TIME,'yyyy-MM') as start_month
from ods_crm.table_case , ods_crm.table_TOPIC , ods_crm.table_intrxn , ODS_REMEDY.COMMON_TICKET tickets ,customer_iptv_subscription
where
table_case. ID_NUMBER = table_TOPIC.X_FOCUS_ID
and table_TOPIC.TOPIC2INTRXN = table_intrxn.objid
and TABLE_CASE.id_number =  tickets.txt_originatingticketid
and cast(tickets.CBP as string) = cast(customer_iptv_subscription.customer_id as string)
and START_TIME  between '${hiveconf:start_date}' and '${hiveconf:end_date}'
and TXT_ACCOUNTNUMBER is not null
and START_TIME is not null
and END_TIME is not null
--and START_TIME  between date_sub(current_date(),60) and current_date()
and table_case.X_PROD_LINE = 'IPTV'
;
!echo vw_techops_customer_calls created successfully;

 with merged_data as (select CONN_ID
,ACCT_NUMBER
,CALLTYPE1
,CALLTYPE2
,CALLTYPE3
,CALLTYPE4
,CALLTYPE5
,LINEOFBUSINESS
,START_TIME
,END_TIME
,DURATION
,SUBSCRIPTION_ID
,HDP_INSERT_TS
,HDP_UPDATE_TS
,START_MONTH
, 1 as sort_order from iptv.vw_techops_customer_calls delta
 UNION ALL select tgt.*, 2 as sort_order from iptv.ignite_customer_calls tgt
 where start_month in (select distinct start_month  from iptv.vw_techops_customer_calls ))

insert overwrite table iptv.ignite_customer_calls  partition (start_month)
 
select CONN_ID
,ACCT_NUMBER
,CALLTYPE1
,CALLTYPE2
,CALLTYPE3
,CALLTYPE4
,CALLTYPE5
,LINEOFBUSINESS
,START_TIME
,END_TIME
,DURATION
,SUBSCRIPTION_ID
,HDP_INSERT_TS
,HDP_UPDATE_TS
,START_MONTH
from (select *, row_number() over ( Partition by Acct_Number,start_time,end_time order by sort_order asc) rank_order from merged_data ) merged_ranked
 where merged_ranked.rank_order = 1;

!echo data loaded successfully;
