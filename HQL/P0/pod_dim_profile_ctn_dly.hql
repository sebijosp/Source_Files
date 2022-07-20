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
