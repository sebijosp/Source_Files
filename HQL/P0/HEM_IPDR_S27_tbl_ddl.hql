!echo ;
!echo 

DROP TABLE IF EXISTS HEM.IPDR_S27;
CREATE TABLE HEM.IPDR_S27(
    CMTS_HOST_NAME            STRING    COMMENT     'Contains the Fully Qualified Domain Name (FQDN) of the CMTS. If the CMTS does not have a domain name, it contains an empty string.',
    CMTS_CM_US_CH_IF_INDEX    BIGINT    COMMENT     'Contains the ifIndex for the upstream interface (described in CmtsCmUsChIfName)',
	DataIuc	                  INT 	    COMMENT     'The DataIuc associated with this upstream OFDMA profile',
	TotalCodewords            BIGINT	COMMENT     'The count of the total number of FEC codewords either received from the CM/CMs on this Profile/Data IUC for this upstream OFDMA channel',
	CorrectedCodewords        BIGINT 	COMMENT     'The count of codewords received that failed the pre-decoding syndrome check, but passed the post-decoding syndrome check from the CM/CMs on this Profile/Data IUC for this upstream OFDMA channel',
	UnreliableCodewords	      BIGINT    COMMENT     'This attribute represents the count of codewords that failed the post-decoding syndrome check received from the CM/CMs on this Profile/Data IUC for this upstream OFDMA channel',
	InOctets                  BIGINT 	COMMENT     'This attribute is the count of unicast octets received by the CCAP on this profile. This value is the size of all unicast frames (including all MAC-layer framing) and CCF PMD overhead (segment headers and stuffing bytes) delivered from the PHY to the MAC - this includes user data, DOCSIS MAC Management Messages, etc. Discontinuities in the value of this counter can occur at reinitialization of the managed system, and at other times as indicated by the value of ProfileCounterDiscontinuityTime',
	CounterDiscontinuityTime  BIGINT    COMMENT     'This attribute is the value of sysUpTime on the most recent occasion at which any one or more of this entrys counters suffered a discontinuity. If no such discontinuities have occurred since the last re-initialization of the local management subsystem, then this attribute contains a zero value',
    RecType                   INT       COMMENT     'typical ipdr record type',
    RecCreationTime           BIGINT    COMMENT     'RecCreationTime',
    CMTS_IP_ADDRESS           STRING     COMMENT   'IP Address off CMTS derived from the file name',
    HDP_INSERT_TS             TIMESTAMP COMMENT     'Timestamp when the record was inserted into Hive',
    HDP_UPDATE_TS             TIMESTAMP COMMENT     'Timestamp when the record was updated in Hive',
    HDP_FILE_NAME             STRING    COMMENT     'Source file name from which the record was extracted',
   JOB_EXECUTION_ID           STRING    COMMENT     'Job Execution ID maps to execution ID of loadcontrol'
)
PARTITIONED BY (PROCESSED_TS TIMESTAMP)
STORED AS ORC;
