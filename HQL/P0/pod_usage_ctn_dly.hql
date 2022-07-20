drop table data_pods.pod_usage_ctn_dly;
CREATE TABLE data_pods.pod_usage_ctn_dly(
  ban bigint, 
  subscriber_no bigint, 
  data_usage double, 
  data_usage_other double, 
  data_usage_roam double, 
  data_total double, 
  vm_ctn_mins_local double, 
  vm_num_calls_local double, 
  vm_ctn_mins_ld_can double, 
  vm_num_calls_ld_can double, 
  vm_ctn_mins_ld_usa double, 
  vm_num_calls_ld_usa double, 
  ctn_mins_local double, 
  num_calls_local double, 
  ctn_mins_ld_can double, 
  num_calls_ld_can double, 
  ctn_mins_ld_usa double, 
  num_calls_ld_usa double)
PARTITIONED BY ( 
  bill_year int, 
  bill_month int);
