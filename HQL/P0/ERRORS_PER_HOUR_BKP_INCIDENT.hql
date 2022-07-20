set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP Table IF EXISTS ext_IPTV.ERRORS_PER_HOUR_BKP_2018_11_06;
CREATE TABLE IF NOT EXISTS ext_IPTV.ERRORS_PER_HOUR_BKP_2018_11_06 like IPTV.ERRORS_PER_HOUR;
INSERT INTO ext_IPTV.ERRORS_PER_HOUR_BKP_2018_11_06 PARTITION(dt_monthly) SELECT * from IPTV.ERRORS_PER_HOUR;
ALTER TABLE  IPTV.ERRORS_PER_HOUR DROP PARTITION(dt_monthly>'2000-01-01');