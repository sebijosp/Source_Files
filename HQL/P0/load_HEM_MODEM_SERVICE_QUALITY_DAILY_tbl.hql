insert overwrite table HEM.MODEM_SERVICE_QUALITY_DAILY partition (event_date)
select
cm_mac_address,
channel_number,
avg(codeword_error_rate),
to_date(event_timestamp)
from hem.MODEM_SERVICE_QUALITY_HOURLY where
event_timestamp > ${hiveconf:DeltaPartStart}
group by
cm_mac_address,
channel_number
to_date(event_timestamp)
