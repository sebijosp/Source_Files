!echo Attempting to drop HEM.UPSTREAM_CHANNEL_DIM;

DROP TABLE IF EXISTS HEM.UPSTREAM_CHANNEL_DIM;

!echo creating HEM.UPSTREAM_CHANNEL_DIM;

CREATE TABLE HEM.UPSTREAM_CHANNEL_DIM(      
   CMTS_NAME             STRING      COMMENT   'Fully Qualified Domain Name (FQDN) of the CMTS',
   MAC_DOMAIN_NAME       STRING      COMMENT   'Contains name of Mac CMTS MAC domain interface',
   MAC_DOMAIN_TYPE       STRING      COMMENT   'Defines the type of node',
   PORT_GROUP            STRING      COMMENT   'Port Group',
   NODE                  STRING      COMMENT   'Return Segment Name',
   CHANNEL_MODULATION    STRING      COMMENT   'Modulation type of the channel',
   CHANNEL_NAME          STRING      COMMENT   'Name of the upstream channel',
   HDP_INSERT_TS         TIMESTAMP,
   HDP_UPDATE_TS         TIMESTAMP,
   HDP_FILE_NAME         STRING,
   JOB_EXECUTION_ID      STRING
)
PARTITIONED BY (RECEIVED_DATE DATE)
STORED AS ORC;

