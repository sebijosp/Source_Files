CREATE TABLE EXT_IPTV.SUBSCRIBER_SQP_IMP_MERGE
(subscriber_no            decimal(9,0),
sys_creation_date        timestamp,
sys_update_date          timestamp,
operator_id              decimal(9,0),
application_id           string,
dl_service_code          string,
dl_update_stamp          decimal(4,0),
sub_seq_no               decimal(9,0),
external_id              string,
customer_id              decimal(9,0),
ch_node_id               decimal(9,0),
effective_date           timestamp,
expiration_date          timestamp,
init_act_date            timestamp,
sub_status               string,
sub_status_date          timestamp,
sub_sts_issue_date       timestamp,
sub_sts_last_act         string,
sub_sts_rsn_cd           string,
dealer_code              string,
initial_dlr_code         string,
sub_password             string,
prim_resource_tp         string,
prim_resource_val        string,
prim_res_param_cd        string,
calc_pym_category        string,
ins_trx_id               decimal(9,0),
trx_id                   decimal(9,0),
conv_run_no              decimal(3,0),
business_entity_id       decimal(9,0),
lang                     string,
link_prev_sub_no         decimal(9,0),
link_next_sub_no         decimal(9,0),
orig_act_date            timestamp,
routing_policy_id        decimal(9,0),
required_partition       string,
l9_sam_key               string,
l9_subscriber_sub_type   string,
l9_equip_sum             decimal(9,0),
subscriber_type          string,
eff_to                   timestamp,
eff_from                 timestamp,
crnt_f                   string,
workflow_run_id          decimal(11,0),
insert_ts                timestamp,
update_ts                timestamp,
ri_check_f               string,
l9_fraud_sus_ind         string,
l9_sus_history           string
)ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
'orc.row.index.stride'='50000',
'orc.stripe.size'='536870912');
