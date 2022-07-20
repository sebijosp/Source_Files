!echo ;
!echo 

DROP TABLE IF EXISTS HEM.IPDR_S25;
CREATE TABLE HEM.IPDR_S25(
   CMTS_HOST_NAME              STRING      COMMENT   'Contains the Fully Qualified Domain Name (FQDN) of the CMTS. If the CMTS does not have a domain name, it contains an empty string.',
   CM_MAC_ADDR                 STRING      COMMENT   'Contains the MAC Address of the CM. If the CM has multiple MAC Addresses, it contains the MAC address associated with the Cable (i.e., RF MAC) interface. When the record is a Group Service Flow, the MAC address should contain a multicast address compliant with either [RFC 2464] or [RFC 1112] as applicable',
   CMTS_CM_US_CH_IF_INDEX      BIGINT      COMMENT   'Contains the ifIndex for the upstream interface (described in CmtsCmUsChIfName)',
   DataIuc	                   BIGINT 	   COMMENT   'The DataIuc associated with this upstream OFDMA profile',
   TotalCodewords              BIGINT      COMMENT   'The count of the total number of FEC codewords either received from the CM/CMs on this Profile/Data IUC for this upstream OFDMA channel',
   CorrectedCodewords          BIGINT 	   COMMENT   'The count of codewords received that failed the pre-decoding syndrome check, but passed the post-decoding syndrome check from the CM/CMs on this Profile/Data IUC for this upstream OFDMA channel',
   UnreliableCodewords	       BIGINT      COMMENT   'This attribute represents the count of codewords that failed the post-decoding syndrome check received from the CM/CMs on this Profile/Data IUC for this upstream OFDMA channel',
   PartialChanReasonCode       BIGINT 	   COMMENT   'This attribute returns the current CM-STATUS Event Code which indicates the reason that this CM is in a Partial Channel state utilizing this Profile on this downstream OFDM/upstream OFDMA channel. A value of 0 indicates that the CM is not currently experiencing Partial Channel involving this Profile on this OFDM/OFDMA channel',
   LastPartialChanTime         BIGINT 	   COMMENT   'This attributes returns the date and time when the MAC indicated that this CM recovered from its most recent Partial Channel incident for this Profile on this downstream OFDM/upstream OFDMA channel',
   LastPartialChanReasonCode   INT	       COMMENT   'This attributes returns the last CM-STATUS Event Code which indicates the reason that this CM was experiencing a Partial Channel event for this Profile on this downstream OFDM/upstream OFDMA channel. A value of 0 indicates that the CM has not experienced a Partial Channel incident involving this Profile on this OFDM/OFDMA channel during the CCAPs maintained history of this CM',
   RecType                     INT         COMMENT   'typical ipdr record type',
   RecCreationTime             BIGINT      COMMENT   'RecCreationTime',
   CMTS_IP_ADDRESS           STRING     COMMENT   'IP Address off CMTS derived from the file name',
   HDP_INSERT_TS               TIMESTAMP   COMMENT   'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS               TIMESTAMP   COMMENT   'Timestamp when the record was updated in Hive',
   HDP_FILE_NAME               STRING      COMMENT   'Source file name from which the record was extracted',
   JOB_EXECUTION_ID            STRING      COMMENT   'Job Execution ID maps to execution ID of loadcontrol'
)
PARTITIONED BY (PROCESSED_TS TIMESTAMP)
STORED AS ORC;
