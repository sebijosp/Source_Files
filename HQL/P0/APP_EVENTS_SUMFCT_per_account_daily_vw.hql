CREATE OR REPLACE VIEW `iptv`.`APP_EVENTS_SUMFCT_per_account_daily_vw` AS
select * from (
select account_source_id as `HHID`,
`EVENT_DATE`,
`MAC_ADDRESS`,
max(case when lower(app_name) like '%netflix%' then 1 else 0 end) as `NetFlix`,
max(case when lower(app_name) like '%youtube%' and lower(app_name) not like '%youtubekids%' then 1 else 0 end) as `YouTube`,
max(case when lower(app_name) like '%youtubekids%' then 1 else 0 end) as `YouTubeKids`
from `iptv`.`app_events_sumfct`
where  event_date = DATE_SUB(CURRENT_DATE(), 1) and (account_source_id is not null and mac_address is not null)
group by account_source_id,
event_date,
mac_address 
order by account_source_id,
mac_address,
event_date) a 
where (a.NetFlix=1 or a.youtube=1 or a.youtubekids=1);
