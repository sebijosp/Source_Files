insert into HEM.DS_OFDM_CHANNEL_CONFIG
select 
   CMTS_HOST_NAME,
   concat('md',CMTS_MD_IF_INDEX%100),
   concat_ws(',',NODES),
   count(DS_IF_NAME),
   NULL as CHANNEL_MODULATION,
   sum(OFDM_CAP_BPS/1000000) as CAPACITY_MBPS,
   '${hiveconf:reportDate}' as REPORT_DATE,
   from_unixtime(unix_timestamp()) as HDP_INSERT_TS,
   from_unixtime(unix_timestamp()) as HDP_UPDATE_TS
from HEM.OFDM_CAPACITY_DIM ofdm 
left join (select CMTS,MAC_DOMAIN,collect_set(RTN_SEG) NODES from HEM.RHSI_TOPOLOGY_DIM where CRNT_FLG='Y' group by CMTS,MAC_DOMAIN) topo
  on trim(ofdm.CMTS_HOST_NAME)=trim(topo.CMTS) and ofdm.CMTS_MD_IF_INDEX=trim(topo.MAC_DOMAIN)
where to_date(HDP_UPDATE_TS) >= '${hiveconf:reportDate}'
group by CMTS_HOST_NAME,CMTS_MD_IF_INDEX,NODES;
