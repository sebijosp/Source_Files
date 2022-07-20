Insert into hem.ccap_us_port_util_weekly_fct_bd PARTITION(date_key) 
 Select
     usg15.week_id as Week_id
   , Max(usg15.network_topology_key) as network_topology_key
   , usg15.cmts_host_name
   , usg15.cmts_md_if_index
   , usg15.us_port
   , usg15.is_port_lvl_flg
   , AVG(usg15.us_util_interval) as us_util_interval
   , Max(usg15.Interface_count) as Interface_count
   , Sum(usg15.us_util_total_mslots) as us_util_total_mslots
   , Sum(usg15.us_util_ucastgranted_mslots) as us_util_ucastgranted_mslots
   , Sum(usg15.us_util_total_cntn_mslots) as us_util_total_cntn_mslots
   , Sum(usg15.us_util_used_cntn_mslots) as us_util_used_cntn_mslots
   , Sum(usg15.us_util_coll_cntn_mslots) as us_util_coll_cntn_mslots
   , Sum(usg15.us_util_total_cntn_req_mslots) as us_util_total_cntn_req_mslots
   , Sum(usg15.us_util_used_cntn_req_mslots) as us_util_used_cntn_req_mslots
   , Sum(usg15.us_util_coll_cntn_req_mslots) as us_util_coll_cntn_req_mslots
   , Sum(usg15.us_util_total_cntn_req_data_mslots) as us_util_total_cntn_req_data_mslots
   , Sum(usg15.us_util_used_cntn_req_data_mslots) as us_util_used_cntn_req_data_mslots
   , Sum(usg15.us_util_coll_cntn_req_data_mslots) as us_util_coll_cntn_req_data_mslots
   , Sum(usg15.us_util_total_cntn_init_maint_mslots) as us_util_total_cntn_init_maint_mslots
   , Sum(usg15.us_util_used_cntn_init_maint_mslots) as us_util_used_cntn_init_maint_mslots
   , Sum(usg15.us_util_coll_cntn_init_maint_mslots) as  us_util_coll_cntn_init_maint_mslots
   , Max(usg15.us_util_index_percentage) as utilization_max
   , Max(case when Rnk = rnk_95 then usg15.us_util_index_percentage else 0 end ) as utilization_95
   , Max(case when Rnk = rnk_98 then usg15.us_util_index_percentage else 0 end ) as utilization_98
   , Sum(usg15.us_bytes) as us_bytes
   , Avg(usg15.us_throughput_bps) as us_throughput_bps
   , Max(usg15.us_throughput_bps) as us_throughput_max
   , Max(usg15.capacity_bps) as capacity_bps
   , Max(case when Rnk = rnk_95 then usg15.us_throughput_bps else 0 end ) as us_throughput_bps_95
   , Max(case when Rnk = rnk_98 then usg15.us_throughput_bps else 0 end ) as us_throughput_bps_98
   , Sum(usg15.ROP) as ROP
   , avg(usg15.us_util_index_percentage) as us_util_index_percentage
   , 'P'  as Time_Flag
  ,current_timestamp as hdp_insert_ts
  ,current_timestamp as hdp_update_ts
  ,3752685 as execution_id
  , date_sub(usg15.dt,   CAST(date_format(usg15.dt ,'u') AS int)-1) as date_key
 from
 (
   Select
     fctrnk.*
     ,ntile(672) over (partition by week_id, cmts_host_name, cmts_md_if_index, us_port, is_port_lvl_flg Order by us_util_index_percentage desc, us_util_ucastgranted_mslots desc) as Rnk
     ,ceil(polls*0.05) as rnk_95
     ,ceil(polls*0.02) as rnk_98
     ,Min(date_key)  over(partition by week_id) as dt
   from
   (
     select
       concat(2020, weekofYear(date_sub(date_key, CAST(date_format(date_key,'u') AS int)-1))) as week_id,
       count(date_key) over(partition by concat(2020, weekofYear(date_sub(date_key, CAST(date_format(date_key,'u') AS int)-1))), cmts_host_name, cmts_md_if_index, us_port, is_port_lvl_flg) as polls
       ,fct.*

     from
       hem.CCAP_US_PORT_UTIL_15_FCT as fct
     where date_key >= '2020-12-28' and date_key < '2021-01-04'
        and is_pt = 1
   ) as fctrnk
 ) as usg15
 Group by
   usg15.week_id
   , usg15.dt
   , usg15.cmts_host_name
   , usg15.cmts_md_if_index
   , usg15.us_port
   , usg15.is_port_lvl_flg;
