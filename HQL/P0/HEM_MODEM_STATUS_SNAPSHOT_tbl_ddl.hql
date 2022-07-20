DROP TABLE IF EXISTS HEM.MODEM_STATUS_SNAPSHOT;
CREATE TABLE HEM.MODEM_STATUS_SNAPSHOT(
   CMTS_HOST_NAME            STRING     COMMENT   'Contains the Fully Qualified Domain Name (FQDN) of the CMTS.',
   CM_MAC_ADDR               STRING     COMMENT   'MAC Address of the CM. In case of multiple MAC Addresses, contains MAC address associated with the Cable (i.e., RF MAC) interface.',
   CMTS_MD_IF_INDEX          BIGINT     COMMENT   'Contains the ifIndex for the CMTS MAC domain interface (described in CmtsMdIfName).',
   CMTS_MD_IF_NAME           STRING     COMMENT   'Contains the first 50 characters of the ifName from the Interfaces Group MIB for the row entry corresponding to the CMTS Mac Domain interface (ifType = 127).',
   CM_REG_STATUS_VALUE       INT        COMMENT   'Current Cable Modem connectivity state,as specified in the OSSI Specification.Returned status information is the CM status as assumed by the CMTS.',
   CM_LAST_REG_TIME          BIGINT     COMMENT   'Contains 32-bit UTC timestamp (# seconds since epoch) value when the CM was last registered.',
   REC_CREATION_TIME         BIGINT     COMMENT   'Contains 64-bit UTC timestamp (# milliseconds since epoch) at the time the data for the record was acquired.',
   HDP_INSERT_TS             TIMESTAMP  COMMENT   'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS             TIMESTAMP  COMMENT   'Timestamp when the record was updated in Hive'
)
STORED AS ORC;
