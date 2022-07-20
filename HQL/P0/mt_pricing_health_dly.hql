drop table mt_pricing.mt_pricing_health_dly purge;
CREATE TABLE mt_pricing.mt_pricing_health_dly(
table_name string,
layer string,
schema string,
total_volume_pct_diff string,
total_volume_pct_diff_limit_crossed string,
volume_segs string,
volume_pct_diff string,
volume_pct_diff_limit_crossed string,
null_volume_columns string,
null_volume_pct_diff string,
null_volume_pct_diff_limit_crossed string,
hdp_update_ts timestamp
)
PARTITIONED BY (
extraction_date date)
tblproperties ("orc.compress"="SNAPPY");

