DROP TABLE IF EXISTS cdrdm.tbaytel_rp10a_tbay_revenue;
CREATE TABLE IF NOT EXISTS cdrdm.tbaytel_rp10a_tbay_revenue(
    ban_company_code            char(9),
    revdet_ban                  decimal(9,0),
    revdet_soc                  char(9),
    revdet_feature_code         char(6),
    revdet_gl_rev_cd            char(6),
    revdet_actv_reason_code     char(6),
    revdet_subscriber_no        varchar(20),
    revdet_actv_amt             decimal(9,2),
    revdet_charge_type          char(1),
    revdet_service_type         char(1),
    revdet_product_type         char(1),
    revdet_gst_amt              decimal(9,2),
    revdet_pst_amt              decimal(9,2),
    revdet_hst_amt              decimal(9,2),
    revdet_qst_amt              decimal(9,2),
    revdet_roam_tax_amt         decimal(9,2)
)
PARTITIONED BY (
    revdet_bill_year int,
    revdet_bill_month int
)
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");
