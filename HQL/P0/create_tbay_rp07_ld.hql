DROP TABLE IF EXISTS cdrdm.tbaytel_rp07_ld;
CREATE TABLE IF NOT EXISTS cdrdm.tbaytel_rp07_ld(
    subscriber_no                   varchar(20),
    call_to_city_desc               varchar(28),
    serve_place                     varchar(28),
    volte_mins                      bigint,
    vowifi_mins                     bigint,
    ld_intl_og_vmin                 bigint,
    ld_intl_og_cnt                  bigint,
    ld_intl_sms_og_cnt              bigint
)
PARTITIONED BY (
    udate string
)
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");
