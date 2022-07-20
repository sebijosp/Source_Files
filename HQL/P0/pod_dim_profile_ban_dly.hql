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


