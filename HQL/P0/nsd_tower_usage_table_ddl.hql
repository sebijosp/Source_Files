CREATE TABLE IF NOT EXISTS NW_TOWER_USAGE.tower_usage(
BAN                                VARCHAR(20),
SUBSCRIBER_NO                      VARCHAR(10),
CITY                               VARCHAR(50),
PROVINCE                           VARCHAR(10),
ERP_SITE_LOCATION_CODE             VARCHAR(15))
PARTITIONED BY (RECORD_DATE        DATE)
STORED AS orc;
