DROP TABLE IF EXISTS cdrdm.tbaytel_rp04_tbay_on_rogers;
CREATE TABLE IF NOT EXISTS cdrdm.tbaytel_rp04_tbay_on_rogers(
    subscriber_no               bigint,
    volte_mins                  bigint,
    vowifi_mins                 bigint,
    vilte_mins                  bigint,
    voice_mins                  bigint,
    sms_cnt                     bigint,
    w4g                         decimal(22,2),
    w3g                         decimal(22,2),
    w2g                         decimal(22,2),
    wother                      decimal(22,2),
    distinct_cid                bigint
)
PARTITIONED BY (
    call_year int,
    call_month int
)
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");

