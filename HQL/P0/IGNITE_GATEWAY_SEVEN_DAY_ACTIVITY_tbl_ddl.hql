create table IPTV.IGNITE_GATEWAY_SEVEN_DAY_ACTIVITY(
current_data_mac_id     string,
mac_address             string,
outage_count            int,
accessibility_full_day_pass    int,
ds_attain_pass         int,
us_attain_pass         int,
reboot_count            int,
calculation_date        date
)stored as ORC;
