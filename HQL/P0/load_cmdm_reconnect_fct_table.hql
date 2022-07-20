set hive.tez.container.size=8192;

with cust_event_dtl as
(
select device.accountsourceid as HHID , EVENT_DATE FROM IPTV.RECONNECT_FCT where  (event_date >='${hiveconf:EVENT_END_DATE_IN}' and event_date <='${hiveconf:EVENT_START_DATE_IN}')  and device.accountsourceid  in (select distinct SUB_HHID from ext_IPTV.RECONNECT_TUPLEO_CUST_TEMP)             
),                                                                                         
combine_cust_evt_dt as
(
select b.account_key, b.account_id, b.subscriber_key, a.HHID ,b.PROD_REF_ID, b.CONFIRMATION_DATE , a.event_date from cust_event_dtl a
left join ext_IPTV.RECONNECT_TUPLEO_CUST_TEMP b
on a.HHID = b.SUB_HHID 
),
pre_final_table as
(
select account_key, account_id, subscriber_key, HHID , PROD_REF_ID, CONFIRMATION_DATE , EVENT_DATE from combine_cust_evt_dt where event_date >= date_add(CONFIRMATION_DATE , -1)
), 
final_table as
(
select account_key, account_id, subscriber_key, HHID , PROD_REF_ID, CONFIRMATION_DATE , MIN(EVENT_DATE) as STB_EARLIEST_ACTIVITY , MAX(EVENT_DATE) as STB_LATEST_ACTIVITY  from pre_final_table group by account_key, account_id, subscriber_key, HHID , PROD_REF_ID, CONFIRMATION_DATE
)
insert OVERWRITE TABLE IPTV.CMDM_RECONNECT_FCT
SELECT  account_key, account_id, subscriber_key , HHID, PROD_REF_ID, CONFIRMATION_DATE, STB_EARLIEST_ACTIVITY , STB_LATEST_ACTIVITY  FROM final_table;
