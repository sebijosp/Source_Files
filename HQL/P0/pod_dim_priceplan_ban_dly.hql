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




