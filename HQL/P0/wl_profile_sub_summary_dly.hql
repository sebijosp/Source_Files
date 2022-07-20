drop table data_pods.wl_profile_sub_summary_dly purge;
CREATE TABLE data_pods.wl_profile_sub_summary_dly(
ecid                    bigint,
ban                     bigint,
subscriber_no           bigint,
sub_status              string,
req_deposit_amt         bigint,
init_activation_date    timestamp,
ctn_tenure              int,
bill_year               int,
bill_month              int,
cycle_start_date        timestamp,
cycle_close_date        timestamp,
rogers_payer_segment_ctn        varchar(70),
rogers_payer_segment_arpu_delta double,
market_province         string,
optout_dm_flag          varchar(1),
optout_em_flag          varchar(1),
optout_sms_flag         varchar(1),
optout_tm_flag          varchar(1),
super_optout_flag       varchar(1),
mkt_flag                varchar(1),
hdp_update_ts           timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");
