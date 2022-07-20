drop table data_pods.wl_profile_acct_summary_dly purge;
CREATE TABLE data_pods.wl_profile_acct_summary_dly(
ecid                    bigint,
ban                     bigint,
contact_id              string,
brand                   string,
account_type            string,
account_sub_type        string,
segment_id              string,
segment_desc            string,
company_code            string,
birth_year              int,
bill_province           string,
bill_city               string,
bill_postal_code        string,
ar_balance              double,
bl_last_pay_date        timestamp,
col_delinq_status       string,
COL_DELINQ_STS_DATE   date,
ban_status              string,
credit_class            string,
idv_ind                 smallint,
evp_ind                 smallint,
lst_init_activation_date        timestamp,
ban_init_activation_date        timestamp,
ban_tenure              int,
bill_year               int,
bill_month              int,
cycle_start_date        timestamp,
cycle_close_date        timestamp,
ban_cnt_sub_lines       double,
anchor_segment          varchar(40),
rogers_payer_segment_ban        varchar(70),
rogers_payer_segment_arpa_delta double,
dao_segment             varchar(1000),
hdp_update_ts           timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");


