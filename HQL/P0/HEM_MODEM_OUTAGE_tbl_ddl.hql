CREATE TABLE HEM.MODEM_OUTAGE(
   CM_MAC_ADDR               STRING     COMMENT   'Contains the MAC Address of the CM. If the CM has multiple MAC Addresses, it contains the MAC address associated with the Cable (i.e., RF MAC) interface. When the record is a Group Service Flow, the MAC address should contain a multicast address compliant with either [RFC 2464] or [RFC 1112] as applicable',
   DOWN_START_TS             TIMESTAMP  COMMENT   'Start time of the outage',
   DOWN_END_TS               TIMESTAMP  COMMENT   'End time of the outage',
   INTERVAL_START_TS         TIMESTAMP  COMMENT   'Start time of 15 min interval',
   INTERVAL_END_TS           TIMESTAMP  COMMENT   'End time of 15 min interval',
   DOWNTIME                  INT        COMMENT   'Seconds the modem was down in the 15 min interval',
   HDP_INSERT_TS             TIMESTAMP  COMMENT   'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS             TIMESTAMP  COMMENT   'Timestamp when the record was updated in Hive',
   JOB_EXECUTION_ID          STRING     COMMENT   'Job Execution ID maps to execution ID of loadcontrol'
)
PARTITIONED BY (INTERVAL_DATE DATE)
STORED AS ORC

