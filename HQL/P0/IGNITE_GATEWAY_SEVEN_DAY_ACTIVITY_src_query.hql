!date +%Y-%m-%dT%H:%M:%S;!echo INFO Hive: Dropping temp table;

DROP TABLE IF EXISTS iptv.ignite_gateway_seven_day_activity_tmp;

!date +%Y-%m-%dT%H:%M:%S;!echo INFO Hive: Creating temp table;

create table iptv.ignite_gateway_seven_day_activity_tmp
(current_data_mac_id     string,
mac_address             string,
outage_count            int,
accessibility_full_day_count    int,
ds_attain_count         int,
us_attain_count         int,
reboot_count            int
) stored as ORC;

!date +%Y-%m-%dT%H:%M:%S;!echo INFO Hive: Loading data into temp table;

insert overwrite table iptv.ignite_gateway_seven_day_activity_tmp
select 
t1.current_data_mac_id,
t2.mac,
sum(t2.outage_count),
sum(t2.accessibility_perc_full_day),
sum(t3.ds_speed_attainable_full),
sum(t3.us_speed_attainable_full),
sum(t4.Reboot_Count)
from iptv.ignite_gateway_dim t1
left join (select mac,
    regexp_replace(billing_src_mac_id,':','') mac_id,
    event_date,
    sum(outage_count) as outage_count,
    sum(CASE WHEN accessibility_perc_full_day = 100.0 then 1 else 0 END) as accessibility_perc_full_day  
    from hem.modem_accessibility_daily
    where event_date >= date_sub(current_date,7)
    group by mac,
    regexp_replace(billing_src_mac_id,':',''),
    event_date
     ) t2
on t1.current_data_mac_id = t2.mac_id
left join (select cm_mac_addr,
    regexp_replace(billing_src_mac_id,':','') mac_id,
    event_date,
    sum(CASE WHEN ds_speed_attainable_full >= 0.972 THEN 1 ELSE 0 END) as ds_speed_attainable_full,
    sum(CASE WHEN us_speed_attainable_full >= 0.944 THEN 1 ELSE 0 END) as us_speed_attainable_full  
    from hem.modem_attainability_daily
    where event_date >= date_sub(current_date,7)
    group by cm_mac_addr,
    regexp_replace(billing_src_mac_id,':',''),
    event_date
     ) t3
on t1.current_data_mac_id = t3.mac_id
left join (select device.deviceSourceId,
    regexp_replace(device.deviceSourceId,':','') mac_id,
    event_date,
    sum(CASE WHEN (application = 'shell' and reasoncode = 1) then 1 else 0 END) as Reboot_Count 
    from iptv.reconnect_fct
    where event_date >= date_sub(current_date,7)
    group by device.deviceSourceId,
    regexp_replace(device.deviceSourceId,':',''),
    event_date
     ) t4
on t1.current_data_mac_id = t4.mac_id
group by t1.current_data_mac_id,
t2.mac;

!date +%Y-%m-%dT%H:%M:%S;!echo INFO Hive: Temp table load completed !;

!date +%Y-%m-%dT%H:%M:%S;!echo INFO Hive: Loading target table !;

insert overwrite table iptv.ignite_gateway_seven_day_activity
select current_data_mac_id,
mac_address,
outage_count,
accessibility_full_day_count,
ds_attain_count,
us_attain_count,
reboot_count,
current_date
from iptv.ignite_gateway_seven_day_activity_tmp;

!date +%Y-%m-%dT%H:%M:%S;!echo INFO Hive: Target table load completed !;

