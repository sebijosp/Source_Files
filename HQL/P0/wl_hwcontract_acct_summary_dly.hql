drop table data_pods.wl_hwcontract_acct_summary_dly;
CREATE TABLE data_pods.wl_hwcontract_acct_summary_dly(
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
lst_hup_year            int,
lst_hup_month           int,
lst_hup_status_date     timestamp,
lst_hup_request_date    timestamp,
nac_year                int,
nac_month               int,
insulated_ind           int,
hdp_update_ts           timestamp)
PARTITIONED BY ( 
  calendar_year int, 
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");
