CREATE TABLE data_pods.wl_chatr_topup_sub_event_daily(
  cust_ban_no decimal(9,0),
  sbscrbr_no string,
  acct_id decimal(10,0),
  txn_dttm timestamp,
  txn_id string,
  vchr_sn string,
  mstr_acct_txn_amt decimal(21,6),
  user_id decimal(10,0),
  sbscrbr_start_dt timestamp,
  price_plan_code string,
  svc_desc string,
  soc string,
  msf decimal(10,2),
  pstl_cd string,
  email_addr_desc string)
PARTITIONED BY (
  run_date date);
