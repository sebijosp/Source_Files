DROP TABLE IF EXISTS cdrdm.tbaytel_rp08_blackberry;
CREATE TABLE IF NOT EXISTS cdrdm.tbaytel_rp08_blackberry(
    subscriber_no                  varchar(20),
    ban_company_code               char(9),
    revdet_soc                     char(9),
    count_soc                      int,
    revdet_actv_reason_code        char(6),
    ban_last_business_name         varchar(60),
    ban_company_name               varchar(40)
)
PARTITIONED BY (
    bill_year int,
    bill_month int
)
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");
