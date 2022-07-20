CREATE TABLE data_pods.c360_health
(table_name string,
layer string,
schema_name string,
row_count bigint,
skipped_attributes string,
number_of_null_cols int,
null_colnames string,
status string,
partition_update_ts timestamp,
hdp_update_ts timestamp)
PARTITIONED BY (calendar_year int,calendar_month int) 
tblproperties ("orc.compress"="SNAPPY");
