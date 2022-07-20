DROP TABLE IF EXISTS cdrdm.tbaytel_rp01_subscriber;
CREATE TABLE IF NOT EXISTS cdrdm.tbaytel_rp01_subscriber(
    company_code                char(9),
    subscriber_no               bigint,
    feature                     varchar(10),
    call_type                   varchar(10),
    volte_voice_in_mins         bigint,
    vowifi_voice_in_mins        bigint,
    voice_in_mins               bigint,
    imsi                        varchar(20),
    sim_code                    varchar(15),
    device_manufacturer         varchar(10),
    device_model                varchar(20),
    device_volte_enabled        varchar(5),
    imei                        varchar(25)
)
PARTITIONED BY (
    call_year  int,
    call_month int
)
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");
