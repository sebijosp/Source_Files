CREATE OR REPLACE VIEW iptv.app_events_account_monthly_vw AS 
select Year_month, 
sum(case when lower(x.app_name) like '%netflix%' then Unique_Users else 0 end) as Netflix,
sum(case when lower(x.app_name) like '%youtube%' then Unique_Users else 0 end) as Youtube,
sum(case when lower(x.app_name) like '%youtubekids%' then Unique_Users else 0 end) as Youtubekids,
sum(case when lower(x.app_name) like '%amazon%' then Unique_Users else 0 end) as Amazon,
sum(case when lower(x.app_name) like '%sportsnet%' then Unique_Users else 0 end) as Sportsnet,
sum(case when lower(x.app_name) like '%halloween%' then Unique_Users else 0 end) as Halloween,
sum(case when lower(x.app_name) like '%holiday%' then Unique_Users else 0 end) as Holiday
from
(select app_Name, from_unixtime(cast(event_timestamp/1000 as BIGINT), 'yyyy-MM') Year_month, count(distinct account) Unique_Users
from iptv.app_events_sumfct where 
 (lower(app_events_sumfct.app_name) like '%netflix%'  or  lower(app_events_sumfct.app_name) like '%youtube%' or lower(app_events_sumfct.app_name) like '%youtubekids%'
or lower(app_events_sumfct.app_name) like '%amazon%' or lower(app_events_sumfct.app_name) LIKE '%sportsnet%' or 
lower(app_events_sumfct.app_name) LIKE '%halloween%' or lower(app_events_sumfct.app_name) LIKE '%holiday%')
and
event_date between  DATE_SUB(CURRENT_DATE(), 91) and DATE_SUB(CURRENT_DATE(), 1)
group by app_Name, from_unixtime(cast(event_timestamp/1000 as BIGINT) ,'yyyy-MM')
) x
group by Year_month
order by Year_month;


