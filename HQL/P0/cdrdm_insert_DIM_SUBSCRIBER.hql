SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;

MSCK REPAIR table ext_cdrdm.dim_subscriber;

insert overwrite table cdrdm.dim_subscriber
select
subscriber_no,
billing_account_number,
product_type,
franchise_tp,
msid,
account_type,
account_sub_type,
subscriber_status,
subscriber_status_start_date,
subscriber_region,
churn_code,
voluntary_desc,
tenure,
tenure_segment,
contract_length_remaining,
life_time_value,
price_plan_soc_code,
price_plan_soc_desc,
price_plan_group_code,
price_plan_group_desc,
price_plan_series_code,
price_plan_series_desc,
price_plan_series_type,
price_plan_series_type_desc,
mkt_price_plan_group,
commitment_status_start_date,
initial_activation_date,
start_date_cleansed,
deactivation_date,
hvc_tag,
churn_score,
upsell_score,
commitment_status_end_date,
commitment_segment,
vintage,
lifetime_value_decile,
contract_length,
platform,
user_date_of_birth,
load_time_stamp,
bill_cycle,
age_group_id,
current_value,
potential_value,
hup_eligibility,
msd_type,
cust_group,
cust_type,
bl_ebpp_ind,
bl_cons_ind,
credit_class,
clm_tag,
mosaic_cluster,
current_subscriber_flag,
revenue_decile,
n_in_1,
mou_decile,
payee_ind,
netwrk
from ext_cdrdm.dim_subscriber;
