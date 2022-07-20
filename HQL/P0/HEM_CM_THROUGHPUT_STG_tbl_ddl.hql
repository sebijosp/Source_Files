!echo Attempting to drop HEM.CM_THROUGHPUT_STG;

DROP TABLE IF EXISTS HEM.CM_THROUGHPUT_STG;

!echo creating HEM.CM_THROUGHPUT_STG;

CREATE TABLE HEM.CM_THROUGHPUT_STG(
  CMTS_HOST_NAME         STRING    COMMENT 'CMTS Host Name',
  CMTS_MD_IF_INDEX       BIGINT    COMMENT 'Contains the ifIndex for the CMTS MAC domain interface (described in CmtsMdIfName).',
  CMTS_MD_IF_NAME        STRING    COMMENT 'Contains ifName from the Interfaces Group MIB for the row entry corresponding to the CMTS Mac Domain interface.',
  CM_MAC_ADDR            STRING    COMMENT 'Cable Modem Mac Address',
  PORT_GROUP             STRING    COMMENT 'Port group to which the Cable modem is connected (Upstream)',
  EVENT_DATE_HOUR_UTC    TIMESTAMP COMMENT 'Timestamp value containing Date and Hour of the event for which throughput is calculated',
  INTERVAL_START         BIGINT    COMMENT 'Start time value in unixtime(sec) of the 15 minute interval for which throughput is calculated',  
  UP_MBYTES              DOUBLE    COMMENT 'Number of MBytes uploaded in last 1 hour',
  DOWN_MBYTES            DOUBLE    COMMENT 'Number of MBytes downloaded in last 1 hour',
  TOTAL_USG_MBYTES       DOUBLE    COMMENT 'SUM of MBytes uploaded and downloaded in a given hour',
  MODEM_MAKE             STRING    COMMENT 'Manufacturer of the cable modem',
  MODEM_MODEL            STRING    COMMENT 'Model of the cable modem',
  PRODUCT_CODE           STRING    COMMENT 'Service Product code associated with the Cable Modem',
  TIER                   STRING    COMMENT '',
  DOWNLOAD_SPEED_KBPS    BIGINT    COMMENT 'Maximum Download Speed for the given Product Code in Mbps',
  UPLOAD_SPEED_KBPS      BIGINT    COMMENT 'Maximum Upload Speed for the given Product Code in Mbps',
  TECHNOLOGY             STRING    COMMENT 'DOCSIS Version of the Cable Modem',
  DS_TUNERS_COUNT        INT       COMMENT 'Number of Downstream Tuners in the Modem. Derived from RHSI_MODEM_MODEL',
  US_TUNERS_COUNT        INT       COMMENT 'Number of Upstream Tuners in the Modem. Derived from RHSI_MODEM_MODEL',
  DOWNLOAD_SPEED_MBPS    DOUBLE    COMMENT 'Maximum Download Speed for the given Product Code',
  UPLOAD_SPEED_MBPS      DOUBLE    COMMENT 'Maximum Upload Speed for the given Product Code',
  REPORT_FLAG            STRING,
  DS_THRUPUT_MD          DOUBLE    COMMENT 'Downstream Throughput based on Mac Domain Capacity',
  DS_THRUPUT_CM          DOUBLE    COMMENT 'Downstream Throughput based on Cable Modem Capacity',
  DSUS_THRUPUT           DOUBLE    COMMENT 'Downstream Throughput due to Upstream Utilization based on the Technology of the Cable Modem',
  US_THROUGHPUT_30       DOUBLE    COMMENT 'Upstream Throughput for DOCSIS 3.0',
  POTENTIAL_IMPACT_DS    INT,
  ACTUAL_IMPACT_DS       INT,
  POTENTIAL_IMPACT_DSUS  INT,
  ACTUAL_IMPACT_DSUS     INT,
  POTENTIAL_IMPACT_US    INT,
  ACTUAL_IMPACT_US       INT,
  POTENTIAL_IMPACT       INT,
  ACTUAL_IMPACT          INT,
  HDP_INSERT_TS          TIMESTAMP,
  HDP_UPDATE_TS          TIMESTAMP
)
PARTITIONED BY (EVENT_DATE_UTC DATE)
STORED AS ORC;
