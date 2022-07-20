!echo ;
!echo Hive : Drop Partitions;


Alter table hem.nonpi_ccap_ds_md_util_daily_fct drop if exists partition (date_key > '1970-01-01');

!echo;
!echo Hive :Insert into the table;

Insert overwrite  table  hem.nonpi_ccap_ds_md_util_daily_fct Partition(date_key)
select network_topology_key,cmts_host_name,cmts_md_if_index,ds_util_interval,interface_count,is_md_lvl_flg,ds_util_total_bytes,ds_util_used_bytes,capacity_bps,ds_throughput_bps_max,ds_throughput_bps_95,ds_throughput_bps_98,ds_util_index_percentage,ds_util_index_percentage_max,ds_util_index_percentage_95,ds_util_index_percentage_98,
rop,time_flag,date_key from hem.ccap_ds_md_util_daily_fct as a where date_key >=  '${hiveconf:DeltaPartStart}' and date_key <  '${hiveconf:DeltaPartEnd}';


!echo ;
!echo Hive : Data loaded; 
