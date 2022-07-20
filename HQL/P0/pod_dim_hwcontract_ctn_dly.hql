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

