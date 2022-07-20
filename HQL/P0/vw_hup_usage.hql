create or replace view data_pods_nonpi.vw_pod_hup_ctn_dly as
select
  ecid , 
  if(BAN IS NOT NULL, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(BAN AS STRING))) AS VARCHAR(64)),NULL) as ban, 
  if(subscriber_no IS NOT NULL, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(subscriber_no AS STRING))) AS VARCHAR(64)),NULL) as subscriber_no,
  brand , 
  account_type , 
  account_sub_type , 
  segment_desc , 
  pplan_series_type , 
  connection_type , 
  pp_prod_type , 
  pp_type_1 , 
  pp_type_4 , 
  pp_payee , 
  idv_ind , 
  evp_ind , 
  sub_status , 
  device_sku , 
  contract_status , 
  contract_start_date , 
  contract_end_date , 
  total_contract_length_mth , 
  subsidy_start_date , 
  subsidy_end_date , 
  subsidy_amt , 
  init_activation_date , 
  eqsub_seq_no , 
  hup_seq_no , 
  hup_status_date , 
  hup_request_date , 
  hup_tier , 
  hup_type , 
  hup_channel_id , 
  post_hup_subsidy_tier , 
  hup_ind , 
  ppc_ind , 
  hup_n_ppc_ind , 
  cycle_start_date , 
  cycle_close_date , 
  bill_year , 
  bill_month,
  calendar_year , 
  calendar_month  from data_pods.pod_hup_ctn_dly;
  
create or replace view DATA_PODS_NONPI.VW_POD_USAGE_CTN_DLY as
select
if(BAN IS NOT NULL, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(BAN AS STRING))) AS VARCHAR(64)),NULL) AS BAN,
if(SUBSCRIBER_NO IS NOT NULL, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(SUBSCRIBER_NO AS STRING))) AS VARCHAR(64)),NULL) AS SUBSCRIBER_NO,
DATA_USAGE
,DATA_USAGE_OTHER
,DATA_USAGE_ROAM
,DATA_TOTAL
,VM_CTN_MINS_LOCAL
,VM_NUM_CALLS_LOCAL
,VM_CTN_MINS_LD_CAN
,VM_NUM_CALLS_LD_CAN
,VM_CTN_MINS_LD_USA
,VM_NUM_CALLS_LD_USA
,CTN_MINS_LOCAL
,NUM_CALLS_LOCAL
,CTN_MINS_LD_CAN
,NUM_CALLS_LD_CAN
,CTN_MINS_LD_USA
,NUM_CALLS_LD_USA 
,BILL_YEAR
,BILL_MONTH from data_pods.pod_usage_ctn_dly;

