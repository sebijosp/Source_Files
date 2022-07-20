insert into HEM.US_SCQAM_CHANNEL_CONFIG
select 
  CMTS_NAME,
  MAC_DOMAIN_NAME,
  PORT_GROUP,
  NODE,
  count(CHANNEL_NAME),
  CHANNEL_MODULATION,
  sum(CASE WHEN MAC_DOMAIN_TYPE LIKE 'RFOG%' THEN 24 ELSE 
           CASE WHEN (split(CHANNEL_NAME,"-")[2]= '6.4') THEN 24 ELSE 12 END
      END) as CAPACITY ,
  '${hiveconf:reportDate}' as REPORT_DATE,
  from_unixtime(unix_timestamp()) as HDP_INSERT_TS,
  from_unixtime(unix_timestamp()) as HDP_UPDATE_TS
from HEM.UPSTREAM_CHANNEL_DIM where received_date = '${hiveconf:reportDate}'
group by
  CMTS_NAME,
  MAC_DOMAIN_NAME,
  PORT_GROUP,
  NODE,
  CHANNEL_MODULATION;
