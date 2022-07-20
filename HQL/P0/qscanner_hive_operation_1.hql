--Adding column to already existing Hive partitioned table
ALTER TABLE data_pods.c360_health ADD columns (refresh_alert string) CASCADE;

-- Rename table
alter table data_pods.c360_health rename to data_pods.c360_health_bkp;

-- Create table with new structure
drop table data_pods.c360_health purge;
CREATE TABLE data_pods.c360_health
(table_name string,
layer string,
schema_name string,
row_count bigint,
skipped_attributes string,
number_of_null_cols int,
null_colnames string,
status string,
duplicate_row_count bigint,
volume_pct_diff_limit_crossed string,
volume_pct_diff double,
refresh_alert string,
partition_update_ts timestamp,
hdp_update_ts timestamp)
PARTITIONED BY (calendar_year int,calendar_month int) 
tblproperties ("orc.compress"="SNAPPY");

-- Insert data data into new table from old table
Insert overwrite table data_pods.c360_health partition(calendar_year, calendar_month)
select 
table_name,
layer,
schema_name,
row_count,
skipped_attributes,
number_of_null_cols,
null_colnames,
status,
duplicate_row_count,
volume_pct_diff_limit_crossed,
volume_pct_diff,
refresh_alert,
partition_update_ts,
hdp_update_ts,
calendar_year,
calendar_month 
from data_pods.c360_health_bkp;
