Insert into hem.modem_accessibility_weekly_bd PARTITION(year,week)
select 
mac
, cmts
, cbu_code
, phub
, shub
, cmts_md_if_index
, cmts_md_if_name
, date_sub('2020-12-29',1) as week_start_date
, sum(outage_count) as outage_count
, sum(up_status_count) as up_status_count
, sum(down_status_count) as down_status_count
, sum(downtime_all_day) as downtime_all
, avg(accessibility_perc_all_day) as accessibility_perc_all
, sum(downtime_prime) as downtime_prime 
, avg(accessibility_perc_prime) as accessibility_perc_prime
, sum(downtime_full_day) as downtime_full
, avg(accessibility_perc_full_day) as accessibility_perc_full
, 604800 - sum(downtime_all_day) as uptime_all
, 302400 - Sum(downtime_full_day) as uptime_full
, 151200 - Sum(downtime_prime) as uptime_prime 
, case when avg(accessibility_perc_all_day) <=25 then '<=25%'
        when (avg(accessibility_perc_all_day) >25 and avg(accessibility_perc_all_day) <=50 ) Then'25%, <=50%'
        when (avg(accessibility_perc_all_day) >50 and avg(accessibility_perc_all_day) <=75 ) Then'50%, <=75%'
        when (avg(accessibility_perc_all_day) >75 and avg(accessibility_perc_all_day) <=80 ) Then'75%, <=80%'
        when (avg(accessibility_perc_all_day) >80 and avg(accessibility_perc_all_day) <=85 ) Then'80%, <=85%'
        when (avg(accessibility_perc_all_day) >85 and avg(accessibility_perc_all_day) <=90 ) Then'85%, <=90%'
        when (avg(accessibility_perc_all_day) >90 and avg(accessibility_perc_all_day) <=92 ) Then'90%, <=92%'
        when (avg(accessibility_perc_all_day) >92 and avg(accessibility_perc_all_day) <=94 ) Then'92%, <=94%'
        when (avg(accessibility_perc_all_day) >94 and avg(accessibility_perc_all_day) <=96 ) Then'94%, <=96%'
        when (avg(accessibility_perc_all_day) >96 and avg(accessibility_perc_all_day) <=98 ) Then'96%, <=98%'
        when (avg(accessibility_perc_all_day) >98 and avg(accessibility_perc_all_day) <100 ) Then'98%, <100%'
        else '100%' end as accessibility_all_bucket_group
, case when avg(accessibility_perc_all_day) =100 then  'Y' else 0 end as mac_availability_all_flag
, case when avg(accessibility_perc_full_day) <=25  Then '<=25%'
        when (avg(accessibility_perc_full_day) >25 and avg(accessibility_perc_full_day) <=50 ) Then '25%, <=50%'
        when (avg(accessibility_perc_full_day) >50 and avg(accessibility_perc_full_day) <=75 ) Then '50%, <=75%'
        when (avg(accessibility_perc_full_day) >75 and avg(accessibility_perc_full_day) <=80 ) Then '75%, <=80%'
        when (avg(accessibility_perc_full_day) >80 and avg(accessibility_perc_full_day) <=85 ) Then '80%, <=85%'
        when (avg(accessibility_perc_full_day) >85 and avg(accessibility_perc_full_day) <=90 ) Then '85%, <=90%'
        when (avg(accessibility_perc_full_day) >90 and avg(accessibility_perc_full_day) <=92 ) Then '90%, <=92%'
        when (avg(accessibility_perc_full_day) >92 and avg(accessibility_perc_full_day) <=94 ) Then '92%, <=94%'
        when (avg(accessibility_perc_full_day) >94 and avg(accessibility_perc_full_day) <=96 ) Then '94%, <=96%'
        when (avg(accessibility_perc_full_day) >96 and avg(accessibility_perc_full_day) <=98 ) Then '96%, <=98%'
        when (avg(accessibility_perc_full_day) >98 and avg(accessibility_perc_full_day) <100 ) Then '98%, <100%'
        else '100%' end as accessibility_full_bucket_group
        
, case when avg(accessibility_perc_full_day) =100 then  'Y' else 0 end as mac_availability_full_flag

, case when avg(accessibility_perc_prime) <=25 then '<=25%'
        when (avg(accessibility_perc_prime) >25 and avg(accessibility_perc_prime) <=50 ) Then '25%, <=50%'
        when (avg(accessibility_perc_prime) >50 and avg(accessibility_perc_prime) <=75 ) Then '50%, <=75%'
        when (avg(accessibility_perc_prime) >75 and avg(accessibility_perc_prime) <=80 ) Then '75%, <=80%'
        when (avg(accessibility_perc_prime) >80 and avg(accessibility_perc_prime) <=85 ) Then '80%, <=85%'
        when (avg(accessibility_perc_prime) >85 and avg(accessibility_perc_prime) <=90 ) Then '85%, <=90%'
        when (avg(accessibility_perc_prime) >90 and avg(accessibility_perc_prime) <=92 ) Then '90%, <=92%'
        when (avg(accessibility_perc_prime) >92 and avg(accessibility_perc_prime) <=94 ) Then '92%, <=94%'
        when (avg(accessibility_perc_prime) >94 and avg(accessibility_perc_prime) <=96 ) Then '94%, <=96%'
        when (avg(accessibility_perc_prime) >96 and avg(accessibility_perc_prime) <=98 ) Then '96%, <=98%'
        when (avg(accessibility_perc_prime) >98 and avg(accessibility_perc_prime) <100 ) Then '98%, <100%'
        else '100%' end as accessibility_perc_prime_bucket_group
        
, case when avg(accessibility_perc_prime) =100 then  'Y' else 0 end as mac_prime_availability_flag
, current_timestamp as hdp_insert_ts
, current_timestamp as hdp_update_ts
, 2020 as year
, 53 as week
from
hem.modem_accessibility_daily
where
event_date >= '2020-12-28' and event_date < '2021-01-04'
group by
mac
, cmts
, cbu_code
, phub
, shub
, cmts_md_if_index
, cmts_md_if_name; 
