!echo Attempting to drop HEM.DS_US_THRPT_MD_STG;

DROP TABLE IF EXISTS HEM.DS_US_THRPT_MD_STG;

!echo creating HEM.DS_US_THRPT_MD_STG;

CREATE TABLE HEM.DS_US_THRPT_MD_STG(
  CMTS_HOST_NAME           STRING      COMMENT "Fully Qualified Domain Name (FQDN) of the CMTS",
  CMTS_MD_IF_INDEX         BIGINT      COMMENT "Contains the ifIndex for the CMTS MAC domain interface",
  PORT_GROUP               STRING      COMMENT "Upstream Port Group",
  INTERVAL_START           BIGINT      COMMENT "Start time of the interval for which throughput is calculated (unixtime sec)",
  EVENT_DATE_HOUR_UTC      TIMESTAMP   COMMENT "Timestamp value containing Date and Hour of the interval in UTC timezone",
  US_CHANNEL_COUNT_WIDE    INT         COMMENT "Count of wide upstream channels",
  US_CHANNEL_COUNT_NARROW  INT         COMMENT "Count of narrow upstream channels",
  US_CHANNEL_COUNT_RFOG    INT         COMMENT "Count of RFOG upstream channels",
  EFF_US_CAPACITY          DOUBLE      COMMENT "Effective Capacity in bits/sec calculated at the Mac Domain level",
  EFF_CAPACITY_MBPS        DOUBLE      COMMENT "Effective Capacity in Mbits/sec calculated at the Mac Domain level",
  EFF_US_UTIL_PCT          DOUBLE      COMMENT "Average Upstream utilization percent",
  THROUGHPUT_CALC_31       DOUBLE      COMMENT "Calculated DS Throughput due to US utilization for DOCSIS 3.1",
  DS_US_THROUGHPUT_30      DOUBLE      COMMENT "Downstream Throughput due to Upstream utilization for DOCSIS 3.0",
  DS_US_THROUGHPUT_31      DOUBLE      COMMENT "Downstream Throughput due to Upstream utilization for DOCSIS 3.1",
  HDP_INSERT_TS            TIMESTAMP,
  HDP_UPDATE_TS            TIMESTAMP
)
PARTITIONED BY (EVENT_DATE_UTC DATE)
STORED AS ORC;
