drop table data_pods.pod_usage_ban_dim_dly;
CREATE TABLE data_pods.pod_usage_ban_dim_dly(
  ecid bigint,
  ban bigint, 
  ban_data_usage double,
  ban_data_usage_other double,
  ban_data_usage_other_kb double,
  ban_data_usage_roam double,
  ban_data_total double,
  ban_vm_ctn_mins_local double,
  ban_vm_num_calls_local double,
  ban_vm_ctn_mins_ld_can double,
  ban_vm_num_calls_ld_can double,
  ban_vm_ctn_mins_ld_usa double,
  ban_vm_num_calls_ld_usa double,
  ban_VM_CTN_MINS_LD_INTL double,
  ban_VM_NUM_CALLS_LD_INTL double,
  ban_ctn_mins_local double,
  ban_num_calls_local double,
  ban_ctn_mins_ld_can double,
  ban_num_calls_ld_can double,
  ban_ctn_mins_ld_usa double,
  ban_num_calls_ld_usa double,
  ban_CTN_MINS_LD_INTL double,
  ban_NUM_CALLS_LD_INTL double,
  hdp_update_ts timestamp)
PARTITIONED BY ( 
  bill_year int, 
  bill_month int);
