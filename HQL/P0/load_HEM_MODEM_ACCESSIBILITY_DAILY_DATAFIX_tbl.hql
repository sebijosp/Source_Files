set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO HEM.MODEM_ACCESSIBILITY_DAILY_DATAFIX PARTITION(EVENT_DATE)
select 
 old.mac ,
 old.cmts,
 old.ACCOUNT as ACCOUNT,
 old.system,
 old.PHUB as PHUB,
 old.SHUB as SHUB,
 old.node,
 old.rtn_seg ,
 old.clamshell ,
 old.smt as SMT,
 old.modem_manufacturer,
 old.model ,
 old.firmware,
 old.bb_tier ,
 old.hh_type ,
 old.contract_type ,
 old.ADDRESS as ADDRESS,
 old.time_zone ,
 old.is_dst,
 old.equipment_status_code,
 old.billing_src_mac_id,
 old.postal_zip_code ,
 NVL(NEW.DOWNTIME_COUNT,old.outage_count) AS outage_count,
 NVL(NEW.up_status_count,old.up_status_count) AS up_status_count,
 NVL(NEW.down_status_count,old.down_status_count) AS down_status_count,
 NVL(NEW.TOTAL_DOWNTIME,old.downtime_all_day) AS downtime_all_day,
 86400 TOTAL_TIME_ALL_DAY,
 ((1-NVL(NEW.TOTAL_DOWNTIME,old.downtime_all_day)/86400) * 100 ) accessibility_perc_all_day,
 NVL(NEW.PRIME_DOWNTIME,old.downtime_prime) AS downtime_prime,
 21600 TOTAL_TIME_PRIME,
 ((1-NVL(NEW.PRIME_DOWNTIME,old.downtime_prime)/21600) * 100 ) accessibility_perc_prime,
 NVL(NEW.FULL_DAY_DOWNTIME,old.downtime_full_day) AS downtime_full_day,
 64800 TOTAL_TIME_FULL,
 ((1-NVL(NEW.FULL_DAY_DOWNTIME,old.downtime_full_day)/64800) * 100 ) accessibility_perc_full_day,
 old.hdp_insert_ts ,
 from_unixtime(unix_timestamp()),
 old.event_date
from HEM.MODEM_ACCESSIBILITY_DAILY old
LEFT JOIN (
SELECT
      MODEM_OUTAGE.CM_MAC_ADDR,
      to_date(from_utc_timestamp(to_utc_timestamp(INTERVAL_START_TS,'America/Toronto'),NVL(SUB.TIMEZONE,'America/Toronto'))) EVENT_DATE,
      SUM(DOWNTIME) TOTAL_DOWNTIME,
      SUM(CASE WHEN hour(from_utc_timestamp(to_utc_timestamp(INTERVAL_START_TS,'America/Toronto'),NVL(SUB.TIMEZONE,'America/Toronto'))) BETWEEN 6 and 24 THEN DOWNTIME ELSE 0 END) FULL_DAY_DOWNTIME,
      SUM(CASE WHEN hour(from_utc_timestamp(to_utc_timestamp(INTERVAL_START_TS,'America/Toronto'),NVL(SUB.TIMEZONE,'America/Toronto'))) BETWEEN 18 and 24 THEN DOWNTIME ELSE 0 END) PRIME_DOWNTIME,
      COUNT(DISTINCT DOWN_START_TS) DOWNTIME_COUNT,
      SUM(CASE WHEN DOWN_START_TS BETWEEN INTERVAL_START_TS and INTERVAL_END_TS THEN 1 ELSE 0 END) as DOWN_STATUS_COUNT,
      SUM(CASE WHEN DOWN_END_TS BETWEEN INTERVAL_START_TS and INTERVAL_END_TS THEN 1 ELSE 0 END) as UP_STATUS_COUNT
   FROM (
      SELECT DISTINCT
         CM_MAC_ADDR,DOWN_START_TS,DOWN_END_TS,INTERVAL_START_TS, INTERVAL_END_TS, INTERVAL_DATE,
         DOWNTIME from HEM.MODEM_OUTAGE_RECOVERY
         WHERE INTERVAL_DATE >= date_sub('${hiveconf:DeltaPartStart}',1)
   )MODEM_OUTAGE
   LEFT JOIN  (
       SELECT DISTINCT MAC, postal_zip_code,TIMEZONE from HEM.MODEM_ACCESSIBILITY_DAILY SUB 
       LEFT JOIN HEM.DB_TIMEZONE_LKP LKP 
       ON SUB.TIME_ZONE = LKP.TIMEZONE_NAME AND SUB.IS_DST=LKP.IS_DST
       where SUB.EVENT_DATE = '2019-07-15' 
   ) SUB
      ON UPPER(MODEM_OUTAGE.CM_MAC_ADDR) = upper(SUB.MAC)
   WHERE substr(from_utc_timestamp(to_utc_timestamp(INTERVAL_START_TS,'America/Toronto'),NVL(SUB.TIMEZONE,'America/Toronto')),0,10) >= '${hiveconf:DeltaPartStart}'
   GROUP BY 
   MODEM_OUTAGE.CM_MAC_ADDR,
   to_date(from_utc_timestamp(to_utc_timestamp(INTERVAL_START_TS,'America/Toronto'),NVL(SUB.TIMEZONE,'America/Toronto')))
) new
 ON UPPER(old.MAC) = UPPER(new.CM_MAC_ADDR)
 and old.event_date = new.EVENT_DATE
where old.event_date >= '${hiveconf:DeltaPartStart}';
