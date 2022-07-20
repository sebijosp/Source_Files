!echo Attempting to drop HEM.MAC_DOMAIN_THROUGHPUT_STG;

DROP TABLE IF EXISTS HEM.MAC_DOMAIN_THROUGHPUT_STG;


!echo creating HEM.MAC_DOMAIN_THROUGHPUT_STG;

CREATE TABLE HEM.MAC_DOMAIN_THROUGHPUT_STG (
   CMTS_HOST_NAME           STRING      COMMENT "Fully Qualified Domain Name (FQDN) of the CMTS",
   CMTS_MD_IF_INDEX         STRING      COMMENT "Contains the ifIndex for the CMTS MAC domain interface",
   PORT_GROUP               STRING      COMMENT "Upstream Port Group",
   EVENT_DATE_HOUR_UTC      TIMESTAMP   COMMENT "Timestamp value containing Date and Hour of the event for which throughput is calculated",
   INTERVAL_START           BIGINT      COMMENT "Start time value in unixtime(sec) of the 15 minute interval for which throughput is calculated",
   DS_UTIL_IDX_PCT_MD_30    DOUBLE      COMMENT "Percent Downstream utilization (excludes OFDM channels)",
   DS_THROUGHPUT_30         DOUBLE      COMMENT "Downstream Throughput for DOCSIS 3.0",
   DS_THROUGHPUT_31         DOUBLE      COMMENT "Downstream Throughput for DOCSIS 3.1",
   DS_US_THROUGHPUT_30      DOUBLE      COMMENT "Downstream Throughput due to Upstream utilization for DOCSIS 3.0",
   DS_US_THROUGHPUT_31      DOUBLE      COMMENT "Downstream Throughput due to Upstream utilization for DOCSIS 3.1",
   US_THROUGHPUT_30         DOUBLE      COMMENT "Upstream Throughput for DOCSIS 3.0",
   HDP_INSERT_TS            TIMESTAMP,
   HDP_UPDATE_TS            TIMESTAMP
)
PARTITIONED BY (EVENT_DATE_UTC DATE)
STORED AS ORC;
