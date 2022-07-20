Create or replace view hem.modem_attainability_daily_vw as
SELECT 
avg(us_speed_attainable_full) avg_us_speed_attainable_full
,avg(ds_speed_attainable_full) avg_ds_speed_attainable_full
,avg(us_speed_attainable_prime) avg_us_speed_attainable_prime
,avg(ds_speed_attainable_prime) avg_ds_speed_attainable_prime
from hem.modem_attainability_daily
where event_date=date_sub(current_date(),1);
