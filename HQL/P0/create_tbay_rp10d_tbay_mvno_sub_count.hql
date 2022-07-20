DROP TABLE IF EXISTS cdrdm.tbaytel_rp10d_tbay_mvno_sub_count;
CREATE TABLE IF NOT EXISTS cdrdm.tbaytel_rp10d_tbay_mvno_sub_count(
    type             varchar(50),
    soc              varchar(9),
    opening          decimal(22,0),
    closing          decimal(22,0)
)
PARTITIONED BY (
    year  int,
    month int
)
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");
