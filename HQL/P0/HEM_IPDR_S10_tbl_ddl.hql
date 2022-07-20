DROP TABLE HEM.IPDR_S10;

CREATE TABLE HEM.IPDR_S10(
   CMTS_HOST_NAME            STRING     COMMENT   'Contains the Fully Qualified Domain Name (FQDN) of the CMTS. If the CMTS does not have a domain name, it contains an empty string.',
   CMTS_SYS_UP_TIME          BIGINT     COMMENT   'Contains a 32-bit count of hundredths of a second since system initialization, in decimal notation.',
   CMTS_MD_IF_NAME           STRING     COMMENT   'Contains the first 50 characters of the ifName from the Interfaces Group MIB for the row entry corresponding to the CMTS Mac Domain interface (ifType = 127).',
   CMTS_MD_IF_INDEX          BIGINT     COMMENT   'Contains the ifIndex for the CMTS MAC domain interface (described in CmtsMdIfName).',
   CM_MAC_ADDR               STRING     COMMENT   'Contains the MAC Address of the CM. If the CM has multiple MAC Addresses, it contains the MAC address associated with the Cable (i.e., RF MAC) interface. When the record is a Group Service Flow, the MAC address should contain a multicast address compliant with either [RFC 2464] or [RFC 1112] as applicable',
   CM_REG_STATUS_ID          BIGINT     COMMENT   'Contains the id value to uniquely identify a CM.',
   CMTS_CM_US_CH_IF_NAME     STRING     COMMENT   'Contains the first 50 characters of the ifName from the Interfaces Group MIB for the row entry corresponding to the CMTS upstream interface (ifType = 129)',
   CMTS_CM_US_CH_IF_INDEX    BIGINT     COMMENT   'Contains the ifIndex for the upstream interface (described in CmtsCmUsChIfName)',
   CMTS_CM_US_CH_ID            INT      COMMENT   'Contains the US channel id as defined in docsIf3MdChCfgChId',
   CMTS_CM_US_MODULATION_TYPE  INT      COMMENT   'Contains the modulation type currently used by this upstream channel.',
   CMTS_CM_US_RX_POWER         INT      COMMENT   'Contains the receive power as perceived for the upstream channel.',
   CMTS_CM_US_SIGNAL_NOISE     INT      COMMENT   'Contains Signal/Noise ratio as perceived for upstream data from the CM.',
   CMTS_CM_US_MICROREFLECTIONS INT      COMMENT   'Contains the microreflections received on this interface. ',
   CMTS_CM_US_EQ_DATA        STRING     COMMENT   'Contains the equalization data for the CM. ',
   CMTS_CM_US_UNERROREDS     BIGINT     COMMENT   'Contains the codewords received without error from the CM.',
   CMTS_CM_US_CORRECTEDS       INT      COMMENT   ' Contains codewords received with correctable errors from the CM.',
   CMTS_CM_US_UNCORRECTABLES   INT       COMMENT  'Contains codewords received with uncorrectable errors from the CM.',
   CMTS_CM_US_HIGH_RESOLUTION_TIMING_OFFSET INT  COMMENT 'Contains the higher resolution timing offset to provide a finer granularity timing offset.',
   CMTS_CM_US_IS_MUTED         INT      COMMENT   'Denotes if the CM upstream channel has been muted via CM-CTRL-REQ or CM-CTRL-RSP message exchange.',
   CMTS_CM_US_RANGING_STATUS   INT      COMMENT   'Contains the ranging status of the CM',
   REC_TYPE                    INT      COMMENT   '1 - Interim (Service Flow is still running). 2- Stop (Completed), 3-Start , 4- Event (Single message record containing all the information)',
   CMTS_IP_ADDRESS           STRING     COMMENT   'IP Address off CMTS derived from the file name',
   HDP_INSERT_TS             TIMESTAMP  COMMENT   'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS             TIMESTAMP  COMMENT   'Timestamp when the record was updated in Hive',
   HDP_FILE_NAME             STRING     COMMENT   'Source file name from which the record was extracted',
   JOB_EXECUTION_ID          STRING     COMMENT   'Job Execution ID maps to execution ID of loadcontrol'
)
PARTITIONED BY (PROCESSED_TS TIMESTAMP)
STORED AS ORC;
