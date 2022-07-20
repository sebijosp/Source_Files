!echo Attempting to drop HEM.DOWNSTREAM_THRPT_31_MD_STG;

DROP TABLE IF EXISTS HEM.DOWNSTREAM_THRPT_31_MD_STG;

!echo creating HEM.DOWNSTREAM_THRPT_31_MD_STG;

CREATE TABLE HEM.DOWNSTREAM_THRPT_31_MD_STG(
  CMTS_HOST_NAME           STRING      COMMENT "Fully Qualified Domain Name (FQDN) of the CMTS",
  CMTS_MD_IF_INDEX         BIGINT      COMMENT "Contains the ifIndex for the CMTS MAC domain interface",
  INTERVAL_START           BIGINT      COMMENT "Start time of the interval for which throughput is calculated (unixtime sec)",
  EVENT_DATE_HOUR_UTC      TIMESTAMP   COMMENT "Timestamp value containing Date and Hour of the interval in UTC timezone",
  DS_OUT_Mbps              DOUBLE      COMMENT "Downstream Utilization in Mbits/sec for the 15 minute interval (includes OFDM channels)",
  DS_EFF_CAPACITY_SCQAM    DOUBLE      COMMENT "Total Effective Capacity of all the SCQAM Channels in bits/sec",
  DS_EFF_CAPACITY_OFDM     DOUBLE      COMMENT "Effective Capacity of the OFDM Channel in bits/sec",
  DS_EFF_CAPACITY_MD_31    DOUBLE      COMMENT "Effective Capacity in bits/sec calculated at the Mac Domain level (SCQAM and OFDM channels)",
  DS_MD_CAPACITY_Mbps_31   DOUBLE      COMMENT "Effective Capacity in Mbits/sec calculated at the Mac Domain level (SCQAM and OFDM channels)",
  DS_THROUGHPUT_CALC_31    DOUBLE      COMMENT "Calculated throughput for DOCSIS 3.1",
  DS_THROUGHPUT_31         DOUBLE      COMMENT "Throughput for DOCSIS 3.1. Min(1190,DS_THROUGHPUT_CALC_31)",
  HDP_INSERT_TS            TIMESTAMP,
  HDP_UPDATE_TS            TIMESTAMP
)
PARTITIONED BY (EVENT_DATE_UTC DATE)
STORED AS ORC;
