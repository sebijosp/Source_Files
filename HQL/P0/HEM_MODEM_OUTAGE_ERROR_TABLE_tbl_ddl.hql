CREATE TABLE HEM.MODEM_OUTAGE_ERROR_TABLE(
   CM_MAC_ADDR               STRING     COMMENT   'Contains the MAC Address of the CM.',
   DOWN_START_TS             TIMESTAMP  COMMENT   'Start time of the outage',
   DOWN_END_TS               TIMESTAMP  COMMENT   'End time of the outage',
   HDP_INSERT_TS             TIMESTAMP  COMMENT   'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS             TIMESTAMP  COMMENT   'Timestamp when the record was updated in Hive'
)
PARTITIONED BY (PROCESSED_DATE DATE)
STORED AS ORC

