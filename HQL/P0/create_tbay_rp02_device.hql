DROP TABLE IF EXISTS cdrdm.tbaytel_rp02_device;
CREATE TABLE IF NOT EXISTS cdrdm.tbaytel_rp02_device(
    type              varchar(50),
    opening           bigint,
    closing           bigint,
    device_enabled    bigint
)
PARTITIONED BY (
    year  int,
    month int
)
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");
