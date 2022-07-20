!echo Attempting to drop HEM.OFDM_CAPACITY_DIM;

DROP TABLE IF EXISTS HEM.OFDM_CAPACITY_DIM;

!echo creating HEM.OFDM_CAPACITY_DIM;

CREATE TABLE HEM.OFDM_CAPACITY_DIM(
  CMTS_HOST_NAME       STRING     COMMENT 'Fully Qualified Domain Name (FQDN) of the CMTS.',
  CMTS_MD_IF_INDEX     INT        COMMENT 'Interface Index for the CMTS MAC domain interface (described in CmtsMdIfName)',
  CMTS_SYS_UP_TIME     BIGINT     COMMENT 'Latest snapshot of 32-bit count of hundredths of a second since system initialization',
  DS_IF_NAME           STRING     COMMENT 'Interface Name from the Interfaces Group MIB corresponding to the CMTS downstream interface.',
  DS_UTIL_TOTAL_BYTES  BIGINT     COMMENT 'Latest snapshot of bytes in the payload portion of MPEG Packets(excludes MPEG header or pointer_field) transported by the DS interface.',
  DELTA_CAPACITY       BIGINT     COMMENT 'DS_UTIL_TOTAL_BYTES difference between previous snapshot and current snapshot',
  DELTA_PERIOD         INT        COMMENT 'CMTS_SYS_UP_TIME difference betwwn previous snapshot and current snapshot in seconds',
  OFDM_CAP_BPS         DOUBLE     COMMENT 'Capacity of ofdm channel calculated over DELTA_PERIOD in bits per second',
  HDP_INSERT_TS        TIMESTAMP  COMMENT 'Timestamp when the record was inserted',
  HDP_UPDATE_TS        TIMESTAMP  COMMENT 'Timestamp when the capacity information was updated.'
)
STORED AS ORC;

