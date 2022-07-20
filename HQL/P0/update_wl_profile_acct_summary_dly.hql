-- Add columns to existing hive table
ALTER table data_pods.wl_profile_acct_summary_dly ADD COLUMNS( COL_DELINQ_STS_DATE date) CASCADE;

-- Rename table
ALTER table data_pods.wl_profile_acct_summary_dly rename to data_pods.wl_profile_acct_summary_dly_bkp; 

-- Create table with new structure
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

-- Insert data data into new table from old table
Insert overwrite table data_pods.wl_profile_acct_summary_dly partition(calendar_year,calendar_month)
select ecid,
ban,
contact_id,
brand,
account_type,
account_sub_type,
segment_id,
segment_desc,
company_code,
birth_year,
bill_province,
bill_city,
bill_postal_code,
ar_balance,
bl_last_pay_date,
col_delinq_status,
COL_DELINQ_STS_DATE,
ban_status,
credit_class,
idv_ind,
evp_ind,
lst_init_activation_date,
ban_init_activation_date,
ban_tenure,
bill_year,
bill_month,
cycle_start_date,
cycle_close_date,
ban_cnt_sub_lines,
anchor_segment,
rogers_payer_segment_ban,
rogers_payer_segment_arpa_delta,
dao_segment,
hdp_update_ts,
calendar_year,
calendar_month
from data_pods.wl_profile_acct_summary_dly_bkp; 

--drop bkp table
 drop table data_pods.wl_profile_acct_summary_dly_bkp purge;
