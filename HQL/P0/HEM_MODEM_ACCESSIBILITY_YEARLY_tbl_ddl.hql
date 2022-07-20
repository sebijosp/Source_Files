DROP TABLE IF EXISTS HEM.MODEM_ACCESSIBILITY_YEARLY;
CREATE TABLE HEM.MODEM_ACCESSIBILITY_YEARLY(
   MAC                         STRING     COMMENT   'MAC address of Cable Modem',
   CMTS                        STRING     COMMENT   'Contains the Fully Qualified Domain Name (FQDN) of the CMTS',
   CBU_CODE                    STRING     COMMENT   'CONTACT BILLING UNIT eg GTA RNB SWO',
   PHUB                        STRING     COMMENT   'Primary HUB',
   SHUB                        STRING     COMMENT   'Secondary HUB',
   CMTS_MD_IF_INDEX            STRING     COMMENT   'cmts domain index',
   CMTS_MD_IF_NAME            STRING     COMMENT   'cmts domain name',
   OUTAGE_COUNT                INT        COMMENT   'Count of the number of outages in entire week period',
   UP_STATUS_COUNT             INT        COMMENT   'Number of time the modem came up in a week window',
   DOWN_STATUS_COUNT           INT        COMMENT   'Number of time the modem went down in a week window',
   DOWNTIME_ALL            INT        COMMENT   'Total number of seconds the modem was down in a 24 hours period (includes maintenance window)',
   --TOTAL_TIME_ALL          INT        COMMENT   'Total number of seconds in a week period (includes maintenance window)',
   ACCESSIBILITY_PERC_ALL  DOUBLE     COMMENT   'Modem uptime percentage in a week period (includes maintenance window)',
   DOWNTIME_PRIME              INT        COMMENT   'Total number of seconds modem was down during Prime time(6:00 PM to 12:00 AM)',
   --TOTAL_TIME_PRIME            INT        COMMENT   'Total number of seconds in the prime period',
   ACCESSIBILITY_PERC_PRIME    DOUBLE     COMMENT   'Modem uptime percentage in prime period',
   DOWNTIME_FULL           INT        COMMENT   'Total number of seconds modem was down for full day (excludes maintenance window)',
   --TOTAL_TIME_FULL         INT        COMMENT   'Total number of seconds for full day (excludes maintenance window)',
   ACCESSIBILITY_PERC_FULL DOUBLE     COMMENT   'Modem uptime percentage for full day (excludes maintenance window)',
   ACCESSIBILITY_ALL_BUCKET_GROUP        STRING     COMMENT   'percentage bucket group based on ACCESSIBILITY_PERC_ALL',
   MAC_AVAILABILITY_ALL_FLAG        STRING     COMMENT   'Y if ACCESSIBILITY_PERC_ALL = 100',
   ACCESSIBILITY_FULL_BUCKET_GROUP        STRING     COMMENT   'percentage bucket group based on ACCESSIBILITY_PERC_FULL',
   MAC_AVAILABILITY_FULL_FLAG        STRING     COMMENT   'Y if ACCESSIBILITY_PERC_FULL = 100',
   ACCESSIBILITY_PERC_PRIME_BUCKET_GROUP        STRING     COMMENT   'bucket where the ACCESSIBILITY_PERC_PRIME falls',
   MAC_PRIME_AVAILABILITY_FLAG        STRING     COMMENT   'Y if accessibility_perc_all_day = 100',
   HDP_INSERT_TS               TIMESTAMP  COMMENT   'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS               TIMESTAMP  COMMENT   'Timestamp when the record was updated in Hive'
)
PARTITIONED BY (YEAR INT)
STORED AS ORC;
