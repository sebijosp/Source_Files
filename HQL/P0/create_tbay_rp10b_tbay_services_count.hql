DROP TABLE IF EXISTS cdrdm.tbaytel_rp10b_tbay_services_count;
CREATE TABLE IF NOT EXISTS cdrdm.tbaytel_rp10b_tbay_services_count(
    ban                     decimal(9,0),
    company_code            char(9),
    soc                     char(9),
    count_soc               int,
    actv_reason_code        char(6),
    last_business_name      varchar(60),
    company_name            varchar(40)
)
PARTITIONED BY (
    bill_year  int,
    bill_month int
)
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");
