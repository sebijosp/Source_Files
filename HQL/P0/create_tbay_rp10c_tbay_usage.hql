DROP TABLE IF EXISTS cdrdm.tbaytel_rp10c_tbay_usage;
CREATE TABLE IF NOT EXISTS cdrdm.tbaytel_rp10c_tbay_usage(
    ban_company_code                char(9),
    ban_last_business_name          varchar(60),
    ban_ban                         decimal(9,0),
    usageftr_soc                    varchar(9),
    usageftr_airtime_feature_cd     char(6),
    usageftr_period_level_code      char(2),
    usageftr_call_type              char(4),
    usageftr_ctn_mins_local         decimal(12,2),
    usageftr_chrg_amt_local         decimal(9,2),
    usageftr_ctn_mins_ld_can        decimal(12,2),
    usageftr_chrg_amt_ld_can        decimal(9,2),
    usageftr_ctn_mins_ld_usa        decimal(12,2),
    usageftr_chrg_amt_ld_usa        decimal(9,2),
    usageftr_ctn_mins_ld_intl       decimal(12,2),
    usageftr_chrg_amt_ld_intl       decimal(9,2)
)
PARTITIONED BY (
    bill_year  int,
    bill_month int
)
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");
