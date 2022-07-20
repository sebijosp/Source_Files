DROP TABLE IF EXISTS cdrdm.tbaytel_rp03_rogers_tbay_data_usage;
CREATE TABLE IF NOT EXISTS cdrdm.tbaytel_rp03_rogers_tbay_data_usage(
    subscriber_type             varchar(25),
    w4g                         decimal(22,2),
    w3g                         decimal(22,2),
    w2g                         decimal(22,2),
    subscriber_count            bigint,
    average_lte                 decimal(22,2),
    average_hspa                decimal(22,2)
)
PARTITIONED BY (
    rpt_year int,
    rpt_month int
)
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");
