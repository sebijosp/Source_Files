set hive.tez.container.size=8192;

with t_cust_rollup as
(
select SUB_HHID, max(CONFIRMATION_DATE) as CONFIRMATION_DATE from ext_IPTV.APP_EVENTS_TUPLEO_CUST_TEMP group by SUB_HHID
),
combine_cust_conf_dt as
(
select a.HHID , a.APP_NAME , a.APP_ACTION, a.EVENT_DATE , b.CONFIRMATION_DATE from ext_IPTV.APP_EVENTS_TUPLEO_EVENT_DT_TEMP a
left join t_cust_rollup b on a.HHID = b.SUB_HHID
),
launch_cnt as
(
select HHID , MIN(EVENT_DATE) as EVENT_DATE , sum(CASE WHEN (event_date >= date_add(CONFIRMATION_DATE , -1) and event_date <= date_add(CONFIRMATION_DATE , +7))  THEN 1 ELSE 0 END) AS APP_LAUNCH_COUNT from combine_cust_conf_dt group by HHID
),
pre_final_table as
(
select b.account_key, b.account_id, b.subscriber_key, a.HHID ,b.PROD_REF_ID, b.CONFIRMATION_DATE , a.EVENT_DATE , a.APP_LAUNCH_COUNT from launch_cnt a
left join ext_IPTV.APP_EVENTS_TUPLEO_CUST_TEMP b
on a.HHID = b.SUB_HHID
),
final_table as 
(
select distinct account_key , account_id , subscriber_key , HHID, PROD_REF_ID , CONFIRMATION_DATE, EVENT_DATE , APP_LAUNCH_COUNT from pre_final_table
)
insert OVERWRITE TABLE IPTV.CMDM_APP_EVENTS_FCT SELECT  account_key , account_id , subscriber_key , HHID, PROD_REF_ID , CONFIRMATION_DATE, EVENT_DATE, APP_LAUNCH_COUNT FROM final_table;
