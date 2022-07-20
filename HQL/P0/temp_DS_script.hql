Insert into hem.ccap_ds_md_util_weekly_fct_bd PARTITION(date_key)
select
    md15.week_id as week_id
  , Max(md15.Network_Topology_Key) as Network_Topology_Key
 	, md15.cmts_host_name
	, md15.cmts_md_if_index
  , avg(md15.ds_util_interval) as ds_util_interval
  , Max(md15.interface_count) as interface_count
  , md15.is_md_lvl_flg
	, Sum(md15.ds_util_total_bytes) as ds_util_total_bytes
	, Sum(md15.ds_util_used_bytes) as ds_util_used_bytes
	, Max(md15.capacity_bps) as capacity_bps
  , max(md15.ds_throughput_bps) as ds_throughput_bps_max

  ,Max(case when Rnk = rnk_95 then md15.ds_throughput_bps else 0 end ) as ds_throughput_bps_95
  ,Max(case when Rnk = rnk_98 then md15.ds_throughput_bps else 0 end ) as ds_throughput_bps_98

  ,Max(md15.ds_util_index_percentage)  as ds_util_index_percentage_max

  ,Max(case when Rnk = rnk_95 then md15.ds_util_index_percentage else 0 end ) as ds_util_index_percentage_95
  ,Max(case when Rnk = rnk_98 then md15.ds_util_index_percentage else 0 end ) as ds_util_index_percentage_98
  , Sum(md15.ROP) as ROP
  ,avg(md15.ds_util_index_percentage)  as ds_util_index_percentage
  ,'A'  as Time_Flag
  ,current_timestamp as hdp_insert_ts
  ,current_timestamp as hdp_update_ts
  ,3752892 as execution_id
  ,date_sub(md15.dt,   CAST(date_format(md15.dt ,'u') AS int)-1) as date_key
from
(
Select
    fctrnk.*
    ,ntile(672) over (partition by week_id, cmts_host_name, cmts_md_if_index,is_md_lvl_flg Order by ds_util_index_percentage desc,ds_throughput_bps desc) as Rnk
    ,ceil(polls*0.05) as rnk_95
    ,ceil(polls*0.02) as rnk_98
    ,Min(date_key)  over(partition by week_id) as dt
  from
  (
    select
      concat(2020, weekofYear(date_sub(date_key, CAST(date_format(date_key,'u') AS int)-1))) as week_id
     ,cast(count(fct.date_key) over(partition by concat(2020, weekofYear(date_sub(date_key, CAST(date_format(date_key,'u') AS int)-1))), cmts_host_name, cmts_md_if_index, is_md_lvl_flg) as Int) as polls
     ,fct.*
    from
      hem.CCAP_DS_MD_UTIL_15_FCT as fct
    where date_key >= '2020-12-28' and date_key < '2021-01-04'
  ) as fctrnk
) as md15
group by
  md15.week_id
, md15.dt
, md15.cmts_host_name
, md15.cmts_md_if_index
, md15.is_md_lvl_flg;


Insert into hem.ccap_ds_md_util_weekly_fct_bd PARTITION(date_key) 
 select
    md15.week_id as week_id
  , Max(md15.Network_Topology_Key) as Network_Topology_Key
 	, md15.cmts_host_name
	, md15.cmts_md_if_index
  , avg(md15.ds_util_interval) as ds_util_interval
  , Max(md15.interface_count) as interface_count
  , md15.is_md_lvl_flg
	, Sum(md15.ds_util_total_bytes) as ds_util_total_bytes
	, Sum(md15.ds_util_used_bytes) as ds_util_used_bytes
	, Max(md15.capacity_bps) as capacity_bps
  , max(md15.ds_throughput_bps) as ds_throughput_bps_max
   ,Max(case when Rnk = rnk_95 then md15.ds_throughput_bps else 0 end ) as ds_throughput_bps_95
   ,Max(case when Rnk = rnk_98 then md15.ds_throughput_bps else 0 end ) as ds_throughput_bps_98

   ,Max(md15.ds_util_index_percentage)  as ds_util_index_percentage_max

   ,Max(case when Rnk = rnk_95 then md15.ds_util_index_percentage else 0 end ) as ds_util_index_percentage_95
   ,Max(case when Rnk = rnk_98 then md15.ds_util_index_percentage else 0 end ) as ds_util_index_percentage_98
   , Sum(md15.ROP) as ROP
   ,avg(md15.ds_util_index_percentage)  as ds_util_index_percentage
   ,'F'  as Time_Flag
  ,current_timestamp as hdp_insert_ts
  ,current_timestamp as hdp_update_ts
  ,3752892 as execution_id
  ,date_sub(md15.dt,   CAST(date_format(md15.dt ,'u') AS int)-1) as date_key
  from
 (
 Select
     fctrnk.*
     ,ntile(672) over (partition by week_id, cmts_host_name, cmts_md_if_index,is_md_lvl_flg Order by ds_util_index_percentage desc,ds_throughput_bps desc) as Rnk
     ,ceil(polls*0.05) as rnk_95
     ,ceil(polls*0.02) as rnk_98
     ,Min(date_key)  over(partition by week_id) as dt
   from
   (
    select
      concat(2020, weekofYear(date_sub(date_key, CAST(date_format(date_key,'u') AS int)-1))) as week_id
     ,cast(count(fct.date_key) over(partition by concat(2020, weekofYear(date_sub(date_key, CAST(date_format(date_key,'u') AS int)-1))), cmts_host_name, cmts_md_if_index, is_md_lvl_flg) as Int) as polls
     ,fct.*
    from
      hem.CCAP_DS_MD_UTIL_15_FCT as fct
    where date_key >= '2020-12-28' and date_key < '2021-01-04'
        and is_fullday =1
   ) as fctrnk
 ) as md15
 group by
   md15.week_id
 , md15.dt
 , md15.cmts_host_name
 , md15.cmts_md_if_index
 , md15.is_md_lvl_flg;


Insert into hem.ccap_ds_md_util_weekly_fct_bd PARTITION(date_key)
 select
    md15.week_id as week_id
  , Max(md15.Network_Topology_Key) as Network_Topology_Key
 	, md15.cmts_host_name
	, md15.cmts_md_if_index
  , avg(md15.ds_util_interval) as ds_util_interval
  , Max(md15.interface_count) as interface_count
  , md15.is_md_lvl_flg
	, Sum(md15.ds_util_total_bytes) as ds_util_total_bytes
	, Sum(md15.ds_util_used_bytes) as ds_util_used_bytes
	, Max(md15.capacity_bps) as capacity_bps
  , max(md15.ds_throughput_bps) as ds_throughput_bps_max
   ,Max(case when Rnk = rnk_95 then md15.ds_throughput_bps else 0 end ) as ds_throughput_bps_95
   ,Max(case when Rnk = rnk_98 then md15.ds_throughput_bps else 0 end ) as ds_throughput_bps_98

   ,Max(md15.ds_util_index_percentage)  as ds_util_index_percentage_max

   ,Max(case when Rnk = rnk_95 then md15.ds_util_index_percentage else 0 end ) as ds_util_index_percentage_95
   ,Max(case when Rnk = rnk_98 then md15.ds_util_index_percentage else 0 end ) as ds_util_index_percentage_98
   , Sum(md15.ROP) as ROP
   ,avg(md15.ds_util_index_percentage)  as ds_util_index_percentage
   ,'P'  as Time_Flag
  ,current_timestamp as hdp_insert_ts
  ,current_timestamp as hdp_update_ts
  ,3752892 as execution_id
  ,date_sub(md15.dt,   CAST(date_format(md15.dt ,'u') AS int)-1) as date_key
  from
 (
 Select
     fctrnk.*
     ,ntile(672) over (partition by week_id, cmts_host_name, cmts_md_if_index,is_md_lvl_flg Order by ds_util_index_percentage desc,ds_throughput_bps desc) as Rnk
     ,ceil(polls*0.05) as rnk_95
     ,ceil(polls*0.02) as rnk_98
     ,Min(date_key)  over(partition by week_id) as dt
   from
   (
    select
      concat(2020, weekofYear(date_sub(date_key, CAST(date_format(date_key,'u') AS int)-1))) as week_id
     ,cast(count(fct.date_key) over(partition by concat(2020, weekofYear(date_sub(date_key, CAST(date_format(date_key,'u') AS int)-1))), cmts_host_name, cmts_md_if_index, is_md_lvl_flg) as Int) as polls
     ,fct.*
    from
      hem.CCAP_DS_MD_UTIL_15_FCT as fct
    where date_key >= '2020-12-28' and date_key < '2021-01-04'
     and is_pt = 1
   ) as fctrnk
 ) as md15
 group by
   md15.week_id
 , md15.dt
 , md15.cmts_host_name
 , md15.cmts_md_if_index
 , md15.is_md_lvl_flg;
 
