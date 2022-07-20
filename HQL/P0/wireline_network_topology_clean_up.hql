insert into hem.wireline_network_topology_dim_tmp Select
  network_topology_key,
  Max(case when dim.cmts_name = 'cmts76.kinc' then 'SWO' 
  when dim.cmts_name = 'cmts76.stth' then 'SWO' 
  when dim.cmts_name = 'cmts76.owsd' then 'SYD' 
  when dim.cmts_name = 'cmts76.utic' then 'SYD' 
  else dim.cbu end ) as Cbu ,  
  Max(case when dim.cmts_name = 'cmts76.kinc' then 'SWO' 
  when dim.cmts_name = 'cmts76.stth' then 'SWO' 
  when dim.cmts_name = 'cmts76.owsd' then 'SYD' 
  when dim.cmts_name = 'cmts76.utic' then 'SYD' 
  else dim.eng_region end ) as eng_region,  
  Max(case when dim.cmts_name = 'cmts76.kinc' then 'PG10' 
  when dim.cmts_name = 'cmts76.stth' then 'PL04' 
  when dim.cmts_name = 'cmts76.owsd' then 'POWN1' 
  when dim.cmts_name = 'cmts76.utic' then 'PP03' 
  else dim.phub end ) as phub,
  max(dim.cmts_Name) as cmts_name,
  max(mac_domain) as mac_domain,
  max(md_if_index) as md_if_index,
  max(port) as port,
  min(port_name) as port_name,
  max(port_type) as port_type,
  topology_level,
  'Y' as crnt_f,
  min(eff_start_dt) as eff_start_dt,
  null as eff_end_dt,
  max(hdp_insert_ts) AS hdp_insert_ts,
  max(hdp_update_ts) AS hdp_update_ts
from 
(
  select * from hem.wireline_network_topology_dim 
  UNION
  Select
    b.nwkey+rnk as nw_key, 
   cbu, eng_region, phub, cmts_name, mac_domain, md_if_index, port, port_name, port_type, Topology_level,
    crnt_f, eff_start_dt, eff_end_dt, hdp_insert_ts, hdp_update_ts
  from 
  (
    select 
      Max(a.cbu )as cbu, Max(a.eng_region) as eng_region ,Max(a.phub) as phub  
      ,a.cmts_name, Null as mac_domain ,null as md_if_index ,null as port , null as port_name ,
      Null as port_type ,'1' as topology_level ,'Y' as crnt_f ,min(a.eff_start_dt) as  eff_start_dt,
      Null as eff_end_dt ,min(a.hdp_insert_ts) as hdp_insert_ts ,Min(a.hdp_update_ts) as hdp_update_ts,
      row_number() over() as rnk
    from 
      hem.wireline_network_topology_dim as a 
    where 
      topology_level = '2'
      and a.cmts_name not in (select distinct cmts_name from hem.wireline_network_topology_dim where Topology_level = '1')
    group by 
      a.cmts_name
  ) as X,
  (
    Select Max(network_topology_key) as nwkey from hem.wireline_network_topology_dim 
  ) as b  
) as dim
where 
  network_topology_key Not in (3182)
group by 
  network_topology_key,topology_level
;
