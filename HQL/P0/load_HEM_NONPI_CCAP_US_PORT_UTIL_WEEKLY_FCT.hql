!echo ;
!echo Hive : Drop Partitions;


Alter table HEM.NONPI_CCAP_us_port_util_weekly_fct drop if exists partition (date_key > '1970-01-01');

!echo;
!echo Hive :Insert into the table;

Insert overwrite  table  HEM.NONPI_CCAP_us_port_util_weekly_fct Partition(date_key)
select week_id,network_topology_key,cmts_host_name,cmts_md_if_index,us_port,us_util_interval,interface_count,is_port_lvl_flg,us_util_total_mslots,us_util_ucastgranted_mslots,us_util_total_cntn_mslots,us_util_used_cntn_mslots,us_util_coll_cntn_mslots,us_util_total_cntn_req_mslots,us_util_used_cntn_req_mslots,us_util_coll_cntn_req_mslots,us_util_total_cntn_req_data_mslots,us_util_used_cntn_req_data_mslots,us_util_coll_cntn_req_data_mslots,us_util_total_cntn_init_maint_mslots,us_util_used_cntn_init_maint_mslots,us_util_coll_cntn_init_maint_mslots,utilization_max,utilization_95,utilization_98,us_bytes,capacity_bps,us_throughput_bps,us_throughput_max,us_throughput_bps_95,us_throughput_bps_98,rop,us_util_index_percentage,time_flag,date_key
 from hem.ccap_us_port_util_weekly_fct as a where date_key >=  '${hiveconf:DeltaPartStart}' and date_key <  '${hiveconf:DeltaPartEnd}';


!echo ;
!echo Hive : Data loaded; 
