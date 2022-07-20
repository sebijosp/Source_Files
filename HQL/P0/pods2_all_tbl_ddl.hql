drop table data_pods.pod_dim_billingaccount;
CREATE TABLE data_pods.pod_dim_billingaccount(
ban                     bigint,
province                string,
city                    string,
postal_code             string,
ar_balance              decimal(9,2),
bl_last_pay_date        timestamp,
COL_DELINQ_STATUS    string,
BAN_STATUS           string,
CREDIT_CLASS         string)
PARTITIONED BY (
  load_dt string)
tblproperties ("orc.compress"="SNAPPY");

drop table data_pods.pod_dim_profile_ban_dly;
CREATE TABLE data_pods.pod_dim_profile_ban_dly(
    ecid bigint,
    ban bigint,
    contact_id string,
    brand string,
    account_type  string,
    account_sub_type string,
    SEGMENT_ID string,
    segment_desc string,
    COMPANY_CODE string,
    birth_year int,
    MARKET_PROVINCE string,
    BILL_PROVINCE string,
    BILL_CITY string,
    BILL_POSTAL_CODE string,
    ar_balance double,
    bl_last_pay_date timestamp,
    COL_DELINQ_STATUS string,
    BAN_STATUS string,
    CREDIT_CLASS string,
    idv_ind smallint,
    evp_ind smallint,
    lst_init_activation_date timestamp,
    ban_init_activation_date timestamp,
    ban_tenure int,
    bill_year int,
    bill_month int,
    cycle_start_date timestamp,
    cycle_close_date timestamp,
    ban_cnt_sub_lines double,
    anchor_segment varchar(40),
    rogers_payer_segment_ban varchar(70),
    rogers_payer_segment_arpa_delta double,
    dao_segment varchar(1000),
    hdp_update_ts timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");


drop table data_pods.pod_dim_profile_ctn_dly;
CREATE TABLE data_pods.pod_dim_profile_ctn_dly(
    ecid bigint,
    ban bigint,
    subscriber_no bigint,
    sub_status string,
    init_activation_date timestamp,
    ctn_tenure int,
    bill_year int,
    bill_month int,
    cycle_start_date timestamp,
    cycle_close_date timestamp,
    rogers_payer_segment_ctn varchar(70),
    rogers_payer_segment_arpu_delta double,
    hdp_update_ts timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");

drop table data_pods.pod_dim_priceplan_ban_dly;
CREATE TABLE data_pods.pod_dim_priceplan_ban_dly(
    ecid                    bigint,
    ban                     bigint,
    ban_cnt_legacy          bigint,
    ban_cnt_nonse           bigint,
    ban_cnt_se              bigint,
    ban_cnt_infinite        bigint,
    BAN_CNT_DOP             bigint,
    ban_cnt_dataonly        bigint,
    ban_cnt_voicedata       bigint,
    ban_cnt_voiceonly       bigint,
    ban_cnt_whp             bigint,
    ban_cnt_inmktpublic     bigint,
    ban_cnt_inmktretention  bigint,
    ban_cnt_inmkttvm        bigint,
    ban_cnt_exppublic       bigint,
    ban_cnt_expretention    bigint,
    ban_cnt_exptvm          bigint,
    ban_plan_service_rc     double,
    ban_plan_hardware_rc    double,
    ban_plan_rc             double,
    ban_non_plan_rc         double,
    ban_discount_rc         double,
    ban_hardware_discount_rc        double,
    ban_mdt_rc              double,
    ban_total_mrc           double,
    ban_dtu_bucket          bigint,
    ban_dtu_bonus_bucket    bigint,
    ban_total_dtu_bucket    bigint,
    ban_data_bucket_mb      bigint,
    ban_total_data_bucket_mb        bigint,
    ban_data_band           string,
    lst_ppc_year            int,
    lst_ppc_month           int,
    lst_ppc_eff_date        timestamp,
    lst_ppc_exp_date        timestamp,
    lst_ppc_request_date    timestamp,
    hdp_update_ts           timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");


drop table data_pods.pod_dim_priceplan_ctn_dly;
CREATE TABLE data_pods.pod_dim_priceplan_ctn_dly(
    ecid                    bigint,
    ban                     bigint,
    subscriber_no           bigint,
    pplan_series_type       smallint,
    price_plan_category     string,
    connection_type         string,
    pp_prod_type            string,
    pp_type_1               string,
    pp_type_4               string,
    pp_type_7               string,
    pp_payee                string,
    pp_code                 string,
    pp_desc                 string,
    PP_GROUP_DESC           string,
    infinite_ind            smallint,
    PULSE_IND               smallint,
    DOP_IND                 smallint,
    retention_ind           smallint,
    subsidy_category        string,
    pp_subsidy_tier         string,
    PP_CAT_CODE             string,
    plan_service_rc         double,
    plan_hardware_rc        double,
    plan_rc                 double,
    non_plan_rc             double,
    discount_rc             double,
    hardware_discount_rc    double,
    mdt_rc                  double,
    total_mrc               double,
    sub_dtu_bucket          bigint,
    sub_dtu_bonus_bucket    bigint,
    sub_total_dtu_bucket    bigint,
    sub_data_bucket_mb      bigint,
    sub_total_data_bucket_mb        bigint,
    hardware_payment_category       string,
    hardware_payment_category_alt   string,
    ppc_year                int,
    ppc_month               int,
    ppc_eff_date            timestamp,
    ppc_exp_date            timestamp,
    ppc_sale_exp_date       timestamp,
    ppc_request_date        timestamp,
    ppc_tenure              int,
    ppc_channel_id          string,
    ppc_channel_type        string,
    hdp_update_ts           timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");


drop table data_pods.pod_dim_hwcontract_ban_dly;
CREATE TABLE data_pods.pod_dim_hwcontract_ban_dly(
    ecid                    bigint,
    ban                     bigint,
    ban_cnt_apple           bigint,
    ban_cnt_android         bigint,
    ban_cnt_contract        bigint,
    ban_cnt_longrenewal     bigint,
    ban_cnt_shortrenewal    bigint,
    ban_cnt_shortexpiry     bigint,
    ban_cnt_longexpiry      bigint,
    ban_cnt_simonly         bigint,
    device_msrp             double,
    device_upfront_amt      double,
    subsidy_amt             double,
    finance_amt             double,
    device_remain_balance   double,
    mthly_device_amt        double,
    administration_chg      double,
    rv_amount               double,
    lst_hup_year                int,
    lst_hup_month               int,
    lst_hup_status_date     timestamp,
    lst_hup_request_date    timestamp,
    nac_year                int,
    nac_month               int,
    hdp_update_ts timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");


drop table data_pods.pod_dim_hwcontract_ctn_dly;
CREATE TABLE data_pods.pod_dim_hwcontract_ctn_dly(
    ecid                    bigint,
    ban                     bigint,
    subscriber_no           bigint,
    device_sku              string,
    device_sku_type         string,
    device_colour_code      string,
    device_manufacturer     string,
    device_model            string,
    device_model_detail     string,
    device_class            string,
    device_network_type     string,
    device_os               string,
    grey_mkt_ind            string,
    imei                    string,
    contract_start_date     timestamp,
    contract_end_date       timestamp,
    contract_remaining_mth  int,
    total_contract_length_mth       int,
    subsidy_start_date      timestamp,
    subsidy_end_date        timestamp,
    contract_status         string,
    device_msrp             double,
    device_upfront_amt      double,
    subsidy_amt             double,
    finance_amt             double,
    device_remain_balance   double,
    mthly_device_amt        double,
    administration_chg      double,
    rv_amount               double,
    rv_exp_rtrn_date        timestamp,
    rv_return_status        string,
    hup_year            int,
    hup_month           int,
    hup_status_date     timestamp,
    hup_request_date    timestamp,
    hup_tenure              int,
    hup_channel_id          string,
    hup_channel_type        string,
    eqsub_seq_no            bigint,
    hup_seq_no              bigint,
    sequence_no             bigint,
    hup_tier                string,
    hup_type                string,
    nac_year                int,
    nac_month               int,
    nac_channel_id          string,
    nac_channel_type        string,
    nac_osp                 string,
    nac_carrier_short_name  string,
    ported                  string,
    interbrand_port         string,
    device_unlock_date      timestamp,
    device_orig_sale_date   timestamp,
    hdp_update_ts timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");


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

drop table data_pods.pod_usage_ctn_dim_dly;
CREATE TABLE data_pods.pod_usage_ctn_dim_dly(
  ecid bigint,
  ban bigint,
  subscriber_no bigint,
  data_usage double,
  data_usage_other double,
  data_usage_other_kb double,
  data_usage_roam double,
  data_total double,
  vm_ctn_mins_local double,
  vm_num_calls_local double,
  vm_ctn_mins_ld_can double,
  vm_num_calls_ld_can double,
  vm_ctn_mins_ld_usa double,
  vm_num_calls_ld_usa double,
  VM_CTN_MINS_LD_INTL double,
  VM_NUM_CALLS_LD_INTL double,
  ctn_mins_local double,
  num_calls_local double,
  ctn_mins_ld_can double,
  num_calls_ld_can double,
  ctn_mins_ld_usa double,
  num_calls_ld_usa double,
  CTN_MINS_LD_INTL double,
  NUM_CALLS_LD_INTL double,
  hdp_update_ts timestamp)
PARTITIONED BY (
  bill_year int,
  bill_month int);

drop table data_pods.pod_revenue_ban_dim_dly;
CREATE TABLE data_pods.pod_revenue_ban_dim_dly(
  ecid bigint,
  ban bigint,
  ban_base double,
  ban_base_data double,
  ban_data_browsing double,
  ban_data_other double,
  ban_data_overage double,
  ban_data_roam double,
  ban_device double,
  ban_ecp double,
  ban_fees double,
  ban_ld_can double,
  ban_ld_us_intl double,
  ban_other double,
  ban_sms double,
  ban_sms_us_intl double,
  ban_voice_overage double,
  ban_voice_roam double,
  ban_wes double,
  ban_hpg double,
  ban_MDT double,
  ban_ONE_TIME_DTU double,
  ban_MM_RPU double,
  ban_rpu double,
  hdp_update_ts timestamp)
PARTITIONED BY (
  bill_year int,
  bill_month int)
tblproperties ("orc.compress"="SNAPPY");


drop table data_pods.pod_revenue_ctn_dim_dly;
CREATE TABLE data_pods.pod_revenue_ctn_dim_dly(
  ecid bigint,
  ban bigint,
  subscriber_no bigint,
  base double,
  base_data double,
  data_browsing double,
  data_other double,
  data_overage double,
  data_roam double,
  device double,
  ecp double,
  fees double,
  ld_can double,
  ld_us_intl double,
  other double,
  sms double,
  sms_us_intl double,
  voice_overage double,
  voice_roam double,
  wes double,
  hpg double,
  MDT double,
  ONE_TIME_DTU double,
  MM_RPU double,
  rpu double,
  hdp_update_ts timestamp)
PARTITIONED BY (
  bill_year int,
  bill_month int)
tblproperties ("orc.compress"="SNAPPY");

drop table data_pods.pod_tran_ppc_ctn_dly;
CREATE TABLE data_pods.pod_tran_ppc_ctn_dly(
     ECID bigint,
     BAN bigint,
     SUBSCRIBER_NO bigint,
     PPC_IND int,
     PPC_DATE_TYPE  string,
     PPC_REQUEST_DATE timestamp,
     PPC_REQUEST_YEAR int,
     PPC_REQUEST_MONTH int,
     PPC_EFF_DATE timestamp,
     PPC_YEAR int,
     PPC_MONTH int,
     HUP_N_PPC_IND int,
     hdp_update_ts timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");

drop table data_pods.pod_tran_hup_ctn_dly;
CREATE TABLE data_pods.pod_tran_hup_ctn_dly(
     ECID bigint,
     BAN bigint,
     SUBSCRIBER_NO bigint,
     HUP_IND int,
     HUP_REQUEST_DATE timestamp,
     HUP_REQUEST_YEAR int,
     HUP_REQUEST_MONTH int,
     HUP_STATUS_DATE timestamp,
     HUP_YEAR int,
     HUP_MONTH int,
     HUP_N_PPC_IND int,
     hdp_update_ts timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");

drop table data_pods.pod_tran_nac_ctn_dly;
CREATE TABLE data_pods.pod_tran_nac_ctn_dly(
     ECID bigint,
     BAN bigint,
     SUBSCRIBER_NO bigint,
     NAC_IND int,
     NAC_DATE_TYPE  string,
     INIT_ACTIVATION_DATE timestamp,
     NAC_YEAR int,
     NAC_MONTH int,
     NAC_OSP  string,
     NAC_CARRIER_SHORT_NAME  string,
     INTERBRAND_PORT  string,
     hdp_update_ts timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");


drop table data_pods.pod_tran_deact_ctn_dly;
CREATE TABLE data_pods.pod_tran_deact_ctn_dly(
     ECID bigint,
     BAN bigint,
     SUBSCRIBER_NO bigint,
     DEACT_IND int,
     DEACT_DATE timestamp,
     DEACT_YEAR int,
     DEACT_MONTH int,
     CHURN_CODE  string,
     CHURN_DESC  string,
     VOLUNTARY_CODE  string,
     VOLUNTARY_DESC  string,
     DEACT_REASON_CODE  string,
     DEACT_REASON_DESC  string,
     PORT_OUT  string,
     INTERBRAND_PORT_OUT  string,
     INTERBRAND_PREPAID_IND  string,
     NSP  string,
     CARRIER_CHURN_CODE  string,
     CARRIER_CHURN_DESC  string,
     CARRIER_SHORT_NAME  string,
     hdp_update_ts timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");













