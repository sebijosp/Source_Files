drop table anchor_segment.ECID_BIRTH_YEAR;
create table anchor_segment.ECID_BIRTH_YEAR(
ecid        bigint,
birth_year  int)
PARTITIONED BY (
process_date    date)
tblproperties ("parquet.compress"="SNAPPY");


drop table anchor_segment.wl_usage;
create table anchor_segment.wl_usage(
ecid        bigint, 
bill_year   int,
bill_month  int,
data_total  double)
PARTITIONED BY (
process_date    date)
tblproperties ("parquet.compress"="SNAPPY");


drop table anchor_segment.ch_usage;
create table anchor_segment.ch_usage(
ecid             bigint, 
TOTAL_USAGE_MB   double,
BUILD_DATE       date)
PARTITIONED BY (
process_date    date)
tblproperties ("parquet.compress"="SNAPPY");


drop table anchor_segment.wl_revenue;
create table anchor_segment.wl_revenue(
ecid        bigint, 
bill_year   int,
bill_month  int,
rpu         double)
PARTITIONED BY (
process_date    date)
tblproperties ("parquet.compress"="SNAPPY");


drop table anchor_segment.ch_ibro;
create table anchor_segment.ch_ibro(
ecid          bigint, 
msf           double,
lob           string,
activity_date date)
PARTITIONED BY (
process_date    date)
tblproperties ("parquet.compress"="SNAPPY");
