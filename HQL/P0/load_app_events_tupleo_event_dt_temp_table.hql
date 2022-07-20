set hive.tez.container.size=8192;

with cust_event_dtl as
(
select device.accountsourceid as HHID , appname as APP_NAME, appaction as APP_ACTION , EVENT_DATE FROM IPTV.APP_EVENTS_FCT where  
((appaction = 'start' and appname = 'netflixbkg.ci') OR (appaction = 'launchingApp' and appname <> 'netflixbkg.ci' ))  and (event_date >= '${hiveconf:EVENT_END_DATE_IN}' and event_date <= '${hiveconf:EVENT_START_DATE_IN}')  and device.accountsourceid  in  
(select distinct SUB_HHID from ext_IPTV.APP_EVENTS_TUPLEO_CUST_TEMP)
),
cust_min_date as
(
select HHID , APP_NAME, APP_ACTION  , MIN(EVENT_DATE) as EVENT_DATE from cust_event_dtl group by HHID , app_name, app_action
)
insert OVERWRITE TABLE ext_IPTV.APP_EVENTS_TUPLEO_EVENT_DT_TEMP SELECT  HHID, APP_NAME, APP_ACTION, EVENT_DATE FROM cust_min_date;
