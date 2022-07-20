!echo Attempting to drop HEM.DOWNSTREAM_THRPT_30_MD_STG;

DROP TABLE IF EXISTS HEM.DOWNSTREAM_THRPT_30_MD_STG;

!echo creating HEM.DOWNSTREAM_THRPT_30_MD_STG;

CREATE TABLE HEM.DOWNSTREAM_THRPT_30_MD_STG(
  CMTS_HOST_NAME           STRING      COMMENT "Fully Qualified Domain Name (FQDN) of the CMTS",
  CMTS_MD_IF_INDEX         BIGINT      COMMENT "Contains the ifIndex for the CMTS MAC domain interface",
  INTERVAL_START           BIGINT      COMMENT "Start time of the interval for which throughput is calculated (unixtime sec)",
  EVENT_DATE_HOUR_UTC      TIMESTAMP   COMMENT "Timestamp value containing Date and Hour of the interval in UTC timezone",
  DS_UTIL_IDX_PCT_MD_30    DOUBLE      COMMENT "Percent Downstream utilization (excludes OFDM channels)",
  DS_CHANNEL_COUNT_30      INT         COMMENT "Count of downstream channels (excludes OFDM channels)",
  DS_THROUGHPUT_30         DOUBLE      COMMENT "Calculated throughput for DOCSIS 3.0",
  HDP_INSERT_TS            TIMESTAMP,
  HDP_UPDATE_TS            TIMESTAMP
)
PARTITIONED BY (EVENT_DATE_UTC DATE)
STORED AS ORC;
