SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

!echo "Inserting data into APP_BACKUP.iptv__PPV_BILLING_FACT_${hiveconf:dt}";
INSERT INTO APP_BACKUP.iptv__PPV_BILLING_FACT_${hiveconf:dt} SELECT * FROM iptv.PPV_BILLING_FACT;

!echo "Inserting data into APP_BACKUP.iptv__vod_billing_fact_${hiveconf:dt}";
INSERT INTO APP_BACKUP.iptv__vod_billing_fact_${hiveconf:dt} SELECT * FROM iptv.vod_billing_fact;
