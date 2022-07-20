insert into HEM.DS_SCQAM_CHANNEL_CONFIG
select 
   CMTS_HOST_NAME,
   concat('md',CMTS_MD_IF_INDEX%100),
   concat_ws(',',NODES),
   count(distinct DS_IF_NAME),
   NULL as CHANNEL_MODULATION,
   count(distinct DS_IF_NAME) * 42884296 * 0.8674 / 1000000 as CAPACITY_MBPS,
   '${hiveconf:reportDate}' as REPORT_DATE,
   from_unixtime(unix_timestamp()) as HDP_INSERT_TS,
   from_unixtime(unix_timestamp()) as HDP_UPDATE_TS
from IPDR.IPDR_S15 s15 
left join (select CMTS,MAC_DOMAIN,collect_set(RTN_SEG) NODES from HEM.RHSI_TOPOLOGY_DIM where CRNT_FLG='Y' group by CMTS,MAC_DOMAIN) topo
  on trim(s15.CMTS_HOST_NAME)=trim(topo.CMTS) and s15.CMTS_MD_IF_INDEX=trim(topo.MAC_DOMAIN)
where upper(ds_if_name) not like '%OFDM%'
   and to_date(processed_ts) = '${hiveconf:reportDate}'
group by CMTS_HOST_NAME,CMTS_MD_IF_INDEX,NODES;
