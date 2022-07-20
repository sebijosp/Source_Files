set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP Table IF EXISTS ext_IPTV.MONTHLY_TUNE_TYPES_BKP_2018_11_06;
CREATE TABLE IF NOT EXISTS ext_IPTV.MONTHLY_TUNE_TYPES_BKP_2018_11_06 like IPTV.MONTHLY_TUNE_TYPES;
INSERT INTO ext_IPTV.MONTHLY_TUNE_TYPES_BKP_2018_11_06 PARTITION(dt_monthly) SELECT * from IPTV.MONTHLY_TUNE_TYPES;
ALTER TABLE  IPTV.MONTHLY_TUNE_TYPES DROP PARTITION(dt_monthly>'2000-01-01');
