!echo Attempting to drop HEM.UPSTREAM_MD_CHANNELCOUNT_REF;

DROP TABLE IF EXISTS HEM.UPSTREAM_MD_CHANNELCOUNT_REF;

!echo creating HEM.UPSTREAM_MD_CHANNELCOUNT_REF;

CREATE TABLE HEM.UPSTREAM_MD_CHANNELCOUNT_REF(      
   CMTS_NAME           STRING      COMMENT "Fully Qualified Domain Name (FQDN) of the CMTS",
   MAC_DOMAIN_NAME     STRING      COMMENT "Contains name of Mac CMTS MAC domain interface",
   US_PORT_GROUP       STRING      COMMENT "Upstrem Port group",
   US_CH_COUNT_WIDE    INT         COMMENT "Count of wide upstream channels",
   US_CH_COUNT_NARROW  INT         COMMENT "Count of narrow upstream channels",
   US_CH_COUNT_RFOG    INT         COMMENT "Count of RFOG upstream channels",
   HDP_INSERT_TS       TIMESTAMP,
   HDP_UPDATE_TS       TIMESTAMP,
   HDP_FILE_NAME       STRING
)
STORED AS ORC;

