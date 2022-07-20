-- Add columns to existing hive table
ALTER table data_pods.wl_profile_sub_summary_dly ADD COLUMNS(REQ_DEPOSIT_AMT bigint) CASCADE;

-- Rename table
ALTER table data_pods.wl_profile_sub_summary_dly rename to data_pods.wl_profile_sub_summary_dly_bkp; 

-- Create table with new structure
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
hdp_update_ts           timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");

-- Insert data data into new table from old table
Insert overwrite table data_pods.wl_profile_sub_summary_dly partition(calendar_year,calendar_month)
select ecid,
ban,
subscriber_no,
sub_status,
req_deposit_amt,
init_activation_date,
ctn_tenure,
bill_year,
bill_month,
cycle_start_date,
cycle_close_date,
rogers_payer_segment_ctn,
rogers_payer_segment_arpu_delta,
market_province,
hdp_update_ts,
calendar_year,
calendar_month
from data_pods.wl_profile_sub_summary_dly_bkp; 

--drop bkp table
 drop table data_pods.wl_profile_sub_summary_dly_bkp purge;
