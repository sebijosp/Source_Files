set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP Table IF EXISTS ext_IPTV.MONTHLY_ACTIVE_ACC_DEVICE_TYPE_BKP_2018_11_06;
CREATE TABLE IF NOT EXISTS ext_IPTV.MONTHLY_ACTIVE_ACC_DEVICE_TYPE_BKP_2018_11_06 like IPTV.MONTHLY_ACTIVE_ACC_DEVICE_TYPE;
INSERT INTO ext_IPTV.MONTHLY_ACTIVE_ACC_DEVICE_TYPE_BKP_2018_11_06 PARTITION(dt_monthly) SELECT * from IPTV.MONTHLY_ACTIVE_ACC_DEVICE_TYPE;
ALTER TABLE  IPTV.MONTHLY_ACTIVE_ACC_DEVICE_TYPE DROP PARTITION(dt_monthly>'2000-01-01');
