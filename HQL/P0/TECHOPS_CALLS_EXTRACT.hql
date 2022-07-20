set hive.exec.compress.output=false;
set mapreduce.output.fileoutputformat.compress=false;
CREATE OR REPLACE VIEW iptv.TECHOPS_EXTRACT_CALLS_VW
AS

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
customer_iptv_subscription.SUBSCRIPTION_ID SUBSCRIPTION_ID
from ods_crm.table_case , ods_crm.table_TOPIC , ods_crm.table_intrxn , ODS_REMEDY.COMMON_TICKET tickets ,customer_iptv_subscription 
where 
table_case. ID_NUMBER = table_TOPIC.X_FOCUS_ID 
and table_TOPIC.TOPIC2INTRXN = table_intrxn.objid  
and TABLE_CASE.id_number =  tickets.txt_originatingticketid  
and cast(tickets.CBP as string) = cast(customer_iptv_subscription.customer_id as string)  
and START_TIME  between '${hiveconf:start_date}' and '${hiveconf:end_date}'
;

INSERT OVERWRITE DIRECTORY '/apps/hive/warehouse/iptv.db/techops_calls_extracts/${hiveconf:log_date}/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
NULL DEFINED AS ''
STORED AS TEXTFILE
select * from iptv.techops_extract_calls_vw;

drop view  iptv.techops_extract_calls_vw;

