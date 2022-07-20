drop table data_pods.pod_dim_billingaccount purge;
CREATE TABLE data_pods.pod_dim_billingaccount(
ban                     bigint,
province                string,
city                    string,
postal_code             string,
ar_balance              decimal(9,2),
bl_last_pay_date        timestamp,
COL_DELINQ_STATUS    string,
BAN_STATUS           string,
CREDIT_CLASS         string,
COL_DELINQ_STS_DATE   date,
CLA                  bigint,
CLM_TAG              string)
PARTITIONED BY (
  load_dt string)
tblproperties ("orc.compress"="SNAPPY");

