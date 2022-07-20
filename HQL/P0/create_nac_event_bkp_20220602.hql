DROP TABLE data_pods.WL_NAC_SUB_EVENT_HRLY;
CREATE TABLE data_pods.WL_NAC_SUB_EVENT_HRLY(
  ban decimal(9,0),
  subscriber_no varchar(20),
  actv_code char(3),
  product_type char(1),
  payee_ind char(1),
  sys_creation_date string,
  trx_seq_no decimal(10,0),
  actv_rsn_code char(5),
  group_trx_seq_no decimal(10,0),
  trx_p1 string,
  trx_p2 string,
  trx_p8 string,
  issue_date string,
  new_imei char(20),
  transaction_type char(3),
  hup_request_date string,
  hup_dealer_code char(5),
  hup_type char(1),
  hup_tier varchar(12),
  program_code char(12),
  new_commit_no_month decimal(3,0),
  hup_seq_no decimal(9,0),
  eqsub_seq_no decimal(9,0),
  swap_req_imei_model varchar(30),
  hup_imei_seq int,
  msf decimal(12,2),
  tax_gst_amt decimal(12,2),
  tax_hst_amt decimal(12,2),
  tax_pst_amt decimal(12,2),
  tax_qst_amt decimal(12,2),
  days_since_last_hup decimal(4,0),
  hardware_chg decimal(12,2),
  acc_ahup_balance decimal(12,2),
  administration_chg decimal(12,2),
  pre_hup_charge decimal(12,2),
  bpo_ban_lvl_credit decimal(12,2),
  voluntary_chg decimal(12,2),
  mandatory_chg decimal(12,2),
  fn_credit_amt decimal(12,2),
  msrp_credit decimal(12,2),
  msrp_price decimal(12,2),
  eqsub_monthly_installment decimal(12,2),
  init_installment_amt decimal(12,2),
  subsidy_amt decimal(12,2),
  rv_amount decimal(12,2),
  init_pp char(9),
  equipment_subsidy_status char(1),
  subsidy_type char(1),
  subsidy_activity char(3),
  soc char(9),
  service_type char(1),
  ins_trx_id decimal(12,0),
  dealer_code char(5),
  account_type char(1),
  account_sub_type char(1),
  franchise_cd string,
  dlr_name varchar(30),
  adr_state_code char(2),
  adr_city varchar(28),
  adr_zip char(10),
  dealer_type varchar(30),
  device_model string,
  manf_code varchar(20),
  sku char(15),
  colour_code string,
  device_class_desc varchar(50),
  segment_desc varchar(80),
  e_mail varchar(180),
  postal_code varchar(10),
  price_plan_type_4 varchar(30),
  pplan_series_type decimal(2,0),
  brand varchar(100))
PARTITIONED BY (
  run_date date);
