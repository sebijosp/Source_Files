set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE HEM.MODEM_ACCESSIBILITY_HOURLY PARTITION (EVENT_TIMESTAMP)
SELECT
   SNAPSHOT.CM_MAC_ADDR,
   SNAPSHOT.CMTS_HOST_NAME,
   SUBSCRIBER.ACCOUNT_NUMBER AS ACCOUNT,
   CASE WHEN SUBSCRIBER.SYSTEM = "SS" THEN SUBSCRIBER.FULL_ACCT_NUM ELSE SUBSCRIBER.ACCOUNT_NUMBER END AS FULL_ACCOUNT_NUMBER,
   SUBSCRIBER.CBU_CODE,
   SUBSCRIBER.SYSTEM,
   SUBSCRIBER.PRIMARY_HUB,
   SUBSCRIBER.SECONDARY_HUB,
   SNAPSHOT.CMTS_MD_IF_INDEX,
   SNAPSHOT.CMTS_MD_IF_NAME,
   NULL,
   NULL,
   NULL,
   SUBSCRIBER.SMT,
   SUBSCRIBER.MANUFACTURER_MAKE,
   SUBSCRIBER.MANUFACTURER_MODEL,
   NULL,
   NULL,
   NULL,
   NULL,
   SUBSCRIBER.MUNICIPALITY,
   TZXREF.TIMEZONE_NAME,
   TZXREF.IS_DST,
   SUBSCRIBER.EQUIPMENT_STATUS_CODE,
   SUBSCRIBER.BILLING_SRC_MAC_ID,
   SUBSCRIBER.POSTAL_ZIP_CODE,
   NVL(OUTAGE.DOWNTIME_COUNT,0) OUTAGE_COUNT,
   NVL(OUTAGE.UP_STATUS_COUNT,0) UP_STATUS_COUNT,
   NVL(OUTAGE.DOWN_STATUS_COUNT,0) DOWN_STATUS_COUNT,
   NVL(OUTAGE.TOTAL_DOWNTIME,0) TOTAL_DOWNTIME,
   3600 TOTAL_TIME,
   ((1-NVL(OUTAGE.TOTAL_DOWNTIME,0)/3600) * 100 ) ACCESSIBILITY_PERC,
   NVL(OUTAGE.PRIME_TIME_DOWNTIME,0) TOTAL_DOWNTIME_PRIME,
   (CASE WHEN from_utc_timestamp(to_utc_timestamp(from_unixtime(unix_timestamp('${hiveconf:DeltaPartStart} ${hiveconf:DeltaPartStart_HH}', 'yyyy-MM-dd HH')),'America/Toronto'),NVL(NULL,'America/Toronto')) >= concat('${hiveconf:DeltaPartStart}',' 18:00:00') and from_utc_timestamp(to_utc_timestamp(from_unixtime(unix_timestamp('${hiveconf:DeltaPartStart} ${hiveconf:DeltaPartStart_HH}', 'yyyy-MM-dd HH')),'America/Toronto'),NVL(NULL,'America/Toronto')) <= concat('${hiveconf:DeltaPartStart}',' 23:59:59') THEN 3600 ELSE 0 END) as TOTAL_TIME_PRIME,
   ((1-NVL(OUTAGE.PRIME_TIME_DOWNTIME,0)/3600) * 100 ) ACCESSIBILITY_PERC_PRIME,
   NVL(OUTAGE.FULL_DAY_DOWNTIME,0) TOTAL_DOWNTIME_FULL,
   (CASE WHEN from_utc_timestamp(to_utc_timestamp(from_unixtime(unix_timestamp('${hiveconf:DeltaPartStart} ${hiveconf:DeltaPartStart_HH}', 'yyyy-MM-dd HH')),'America/Toronto'),NVL(NULL,'America/Toronto')) >= concat('${hiveconf:DeltaPartStart}',' 06:00:00') and from_utc_timestamp(to_utc_timestamp(from_unixtime(unix_timestamp('${hiveconf:DeltaPartStart} ${hiveconf:DeltaPartStart_HH}', 'yyyy-MM-dd HH')),'America/Toronto'),NVL(NULL,'America/Toronto')) <= concat('${hiveconf:DeltaPartStart}',' 23:59:59') THEN 3600 ELSE 0 END) as TOTAL_TIME_FULL,
   ((1-NVL(OUTAGE.FULL_DAY_DOWNTIME,0)/3600) * 100 ) ACCESSIBILITY_PERC_FULL,
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp('${hiveconf:DeltaPartStart} ${hiveconf:DeltaPartStart_HH}', 'yyyy-MM-dd HH')) AS EVENT_TIMESTAMP
FROM HEM.MODEM_STATUS_SNAPSHOT SNAPSHOT
LEFT JOIN HEM.SUBSCRIBER_MODEM_DIM_MVW SUBSCRIBER
   ON UPPER(SNAPSHOT.CM_MAC_ADDR) = UPPER(SUBSCRIBER.CM_MAC_ADDR)
LEFT JOIN (select ZIPCODE,TIMEZONE_NAME,IS_DST from HEM.TIMEZONE_ZIPCODE_XREF where PREFFERED='P') TZXREF
   ON substr(TZXREF.ZIPCODE,0,3) = substr(SUBSCRIBER.POSTAL_ZIP_CODE,0,3)
   AND substr(TZXREF.ZIPCODE,-3) = substr(SUBSCRIBER.POSTAL_ZIP_CODE,-3)
LEFT JOIN (
   SELECT
      MODEM_OUTAGE.CM_MAC_ADDR,
      INTERVAL_TIMESTAMP,
      SUM(DOWNTIME) TOTAL_DOWNTIME,
      SUM(CASE WHEN from_utc_timestamp(to_utc_timestamp(INTERVAL_START_TS,'America/Toronto'),NVL(LKP.TIMEZONE,'America/Toronto')) >= concat('${hiveconf:DeltaPartStart}',' 06:00:00') and from_utc_timestamp(to_utc_timestamp(INTERVAL_START_TS,'America/Toronto'),NVL(LKP.TIMEZONE,'America/Toronto')) <= concat('${hiveconf:DeltaPartStart}',' 23:59:59') THEN DOWNTIME ELSE 0 END) FULL_DAY_DOWNTIME,
      SUM(CASE WHEN from_utc_timestamp(to_utc_timestamp(INTERVAL_START_TS,'America/Toronto'),NVL(LKP.TIMEZONE,'America/Toronto')) >= concat('${hiveconf:DeltaPartStart}',' 18:00:00') and from_utc_timestamp(to_utc_timestamp(INTERVAL_START_TS,'America/Toronto'),NVL(LKP.TIMEZONE,'America/Toronto')) <= concat('${hiveconf:DeltaPartStart}',' 23:59:59') THEN DOWNTIME ELSE 0 END) PRIME_TIME_DOWNTIME,
      COUNT(DISTINCT DOWN_START_TS) DOWNTIME_COUNT,
      SUM(CASE WHEN DOWN_START_TS BETWEEN INTERVAL_START_TS and INTERVAL_END_TS THEN 1 ELSE 0 END) as DOWN_STATUS_COUNT,
      SUM(CASE WHEN DOWN_END_TS BETWEEN INTERVAL_START_TS and INTERVAL_END_TS THEN 1 ELSE 0 END) as UP_STATUS_COUNT
   FROM (
      SELECT DISTINCT
         CM_MAC_ADDR,DOWN_START_TS,DOWN_END_TS,INTERVAL_START_TS, INTERVAL_END_TS, INTERVAL_DATE, date_format(INTERVAL_START_TS, 'yyyy-MM-dd HH:00:00') as  INTERVAL_TIMESTAMP, DOWNTIME from HEM.MODEM_OUTAGE
         WHERE INTERVAL_DATE >= date_sub('${hiveconf:DeltaPartStart}',1) AND INTERVAL_DATE <= date_add('${hiveconf:DeltaPartStart}',1) AND date_format(INTERVAL_START_TS, 'yyyy-MM-dd HH') = '${hiveconf:DeltaPartStart} ${hiveconf:DeltaPartStart_HH}'
   )MODEM_OUTAGE
   LEFT JOIN HEM.SUBSCRIBER_MODEM_DIM_MVW SUB
      ON MODEM_OUTAGE.CM_MAC_ADDR = SUB.CM_MAC_ADDR
   LEFT JOIN (select ZIPCODE,TIMEZONE_NAME,IS_DST from HEM.TIMEZONE_ZIPCODE_XREF where PREFFERED='P') XREF
      ON XREF.ZIPCODE = SUB.POSTAL_ZIP_CODE
   LEFT JOIN HEM.DB_TIMEZONE_LKP LKP
      ON XREF.TIMEZONE_NAME = LKP.TIMEZONE_NAME AND XREF.IS_DST=LKP.IS_DST
   WHERE substr(from_utc_timestamp(to_utc_timestamp(INTERVAL_START_TS,'America/Toronto'),NVL(LKP.TIMEZONE,'America/Toronto')),0,13) = '${hiveconf:DeltaPartStart} ${hiveconf:DeltaPartStart_HH}'
   GROUP BY MODEM_OUTAGE.CM_MAC_ADDR, INTERVAL_TIMESTAMP
) OUTAGE
   ON OUTAGE.CM_MAC_ADDR = SNAPSHOT.CM_MAC_ADDR
WHERE SUBSCRIBER.SYSTEM IS NOT NULL;
