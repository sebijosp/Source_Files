insert overwrite table HEM.MODEM_SERVICE_QUALITY_HOURLY partition (event_timestamp)
select
cm_mac_addr,
substring(split(cmts_cm_us_ch_if_name,'/')[1],locate('.',split(cmts_cm_us_ch_if_name,'/')[1])+1,1) as channel_num ,
sum(cmts_cm_us_uncorrectables)/(sum(cmts_cm_us_uncorrectables)+sum(cmts_cm_us_correcteds)+sum(cmts_cm_us_unerroreds)) as CER,
from_unixtime(unix_timestamp(substring(hdp_file_name,locate('s10',hdp_file_name)+4,10),'yyyyMMddHH'))
from ipdr.ipdr_s10 where
processed_ts > ${hiveconf:DeltaPartStart}
group by cm_mac_addr,
substring(split(cmts_cm_us_ch_if_name,'/')[1],locate('.',split(cmts_cm_us_ch_if_name,'/')[1])+1,1),
from_unixtime(unix_timestamp(substring(hdp_file_name,locate('s10',hdp_file_name)+4,10),'yyyyMMddHH'));

