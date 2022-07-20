!echo Attempting to drop HEM.MAC_PORTGROUP_MAPPING_FCT;

DROP TABLE IF EXISTS HEM.MAC_PORTGROUP_MAPPING_FCT;

!echo creating HEM.MAC_PORTGROUP_MAPPING_FCT;

CREATE TABLE HEM.MAC_PORTGROUP_MAPPING_FCT(
   CMTS_HOST_NAME        STRING,
   CM_MAC_ADDR           STRING    COMMENT "Mac address of a cable modem MM-MM-MM-SS-SS-SS",
   PORT_GROUP            STRING    COMMENT "Port Group the Cable Modem is connected to",
   EVENT_DATE_HOUR_UTC   TIMESTAMP,
   HDP_INSERT_TS         TIMESTAMP,
   HDP_UPDATE_TS         TIMESTAMP
) PARTITIONED BY (EVENT_DATE_UTC DATE)
STORED AS ORC;
