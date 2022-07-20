drop table data_pods.WL_PPC_SUB_EVENT_HRLY;
CREATE TABLE data_pods.WL_PPC_SUB_EVENT_HRLY (
    trx_seq_no              bigint,
    ban                     bigint,
    subscriber_no           string,
    event_dttm              timestamp,
    soc                     string,
    soc_description         string,
    soc_seq_no              bigint,
    soc_ver_no              bigint,
    soc_pp_rate             decimal(9,2),
    ppc_status              string,
    expired_soc             string,
    expired_soc_description string,
    expired_soc_seq_no      bigint,
    expired_soc_ver_no      bigint,
    expired_soc_pp_rate     decimal(9,2),
    dealer_code             string,
    effective_issue_date    timestamp,
    soc_effective_date      timestamp,
    expiration_issue_date   timestamp,
    expiration_date         timestamp,
    product_type            string,
    service_type            string,
    account_type            string,
    account_sub_type        string,
    dlr_name                string,
    dlr_adr_state_code      string,
    dlr_adr_city            string,
    dlr_adr_zip             string,
    dealer_type             string,
    dlr_tp_cd               string,
    segment_id              string,
    segment_desc            string,
    price_plan_type_4       string,
    pplan_series_type       bigint,
    brand                   string,
    e_mail                   string,
    hdp_update_ts           timestamp
)
PARTITIONED BY ( 
    run_date                date)
tblproperties ("orc.compress"="SNAPPY");

