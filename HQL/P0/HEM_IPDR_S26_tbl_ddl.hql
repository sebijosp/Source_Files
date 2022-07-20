DROP TABLE IF EXISTS HEM.IPDR_S26;
CREATE TABLE HEM.IPDR_S26(
    CMTS_HOST_NAME                      STRING      COMMENT     'Contains the Fully Qualified Domain Name (FQDN) of the CMTS. If the CMTS does not have a domain name, it contains an empty string.',
    CM_MAC_ADDR                         STRING      COMMENT     'Contains the MAC Address of the CM. If the CM has multiple MAC Addresses, it contains the MAC address associated with the Cable (i.e., RF MAC) interface. When the record is a Group Service Flow, the MAC address should contain a multicast address compliant with either [RFC 2464] or [RFC 1112] as applicable',
    CMTS_CM_US_CH_IF_INDEX              BIGINT      COMMENT     'Contains the ifIndex for the upstream interface (described in CmtsCmUsChIfName)',
    CmLastRegTime                       BIGINT	    COMMENT     'CM LAST REG TIME',
    RXPOWER                             INT 	    COMMENT     'This attribute is the total received power in a specified OFDMA channel, normalized to power in a 1.6 MHz bandwidth, at the RF input port of the CMTS for a given CM',
    MeanRxMer	                        INT 	    COMMENT     'This attribute is the mean of the dB values of the RxMER measurements of all active subcarriers. The mean is computed directly on the dB values as follows: Mean = sum of (RxMER dB values) / number of RxMER values',
    StdDevRxMer	                        INT         COMMENT	    'This attribute is the standard deviation of the dB values of the RxMER measurements of all active subcarriers. The standard deviation is computed directly on the dB values as follows: StdDev = sqrt(sum of (RxMER dB values - RxMER_mean)^2 / number of RxMER values)',
    RxMerThreshold	                    INT	        COMMENT     'This attribute specifies the percentile (such as 2nd percentile or 5th percentile) of all active subcarriers in an OFDM channel at which the ThresholdRxMerValue occurs. That is, (Percentile) % of the subcarriers have RxMER less than or equal to the ThresholdRxMerValue',
    ThresholdRxMerValue	                INT 	    COMMENT     'This attribute is the RxMER value corresponding to the specified RxMerThreshold percentile value. The CCAP sorts the subcarriers in ascending order of RxMER, resulting in a post-sorting subcarrier index ranging from 1 to the number of active subcarriers.',
    ThresholdRxMerHighestFreq           INT	        COMMENT     'This attribute is the frequency in Hz of the highest-frequency subcarrier having RxMER = ThresholdRxMer value',
    CmtsCmUsMicroreflections	        INT		    COMMENT     'Contains the microreflections received on this interface',
    CmtsCmUsHighResolutionTimingOffset	INT	        COMMENT     'Contains the higher resolution timing offset to provide a finer granularity timing offset',
    CmtsCmUsIsMuted	                    INT 	    COMMENT     'Denotes if the CM upstream channel has been muted via CM-CTRL-REQ or CM-CTRL-RSP message exchange',
    CmtsCmUsRangingStatus               INT 	    COMMENT     '1- other, 2- ABORTED, 3- RETRIES EXCEEDED, 4-SUCCESS, 5- CONTINUE',
    CurPartialSvcReasonCode	            INT 	    COMMENT     'This attribute indicates the type of bonding group issue that this CM is experiencing, based on what the MAC-layer shows',
    LastPartialSvcTime	                BIGINT 	    COMMENT     'This attribute returns the date and time when the MAC indicated that this CM recovered from its most recent Partial Service incident on this downstream OFDM/upstream OFDMA channel',
    LastPartialSvcReasonCode	        INT 	    COMMENT     'This attribute returns the last CM-STATUS Event Code which indicates the reason that this CM was experiencing Partial Service on this downstream OFDM/upstream OFDMA channel. (Note: if the CM is currently experiencing Partial Service, this is the Event Code from the previous Partial Service event.) A value of 0 indicates that the CM has not experienced Partial Service involving this OFDM/OFDMA channel during the CCAPs history of this CM',
    NumPartialSvcIncidents              INT	        COMMENT     'This attribute returns the number of Partial Service incidents the MAC layer has reported for this CM on this downstream OFDM/upstream OFDMA channel',
    NumPartialChanIncidents	            INT 	    COMMENT     'This attributes returns the number of Partial Channel incidents the MAC layer has reported for this CM on this downstream OFDM/upstream OFDMA channel',
	RecType                             INT         COMMENT     'typical ipdr record type',
   RecCreationTime                      BIGINT      COMMENT     'RecCreationTime',
   CMTS_IP_ADDRESS           STRING     COMMENT   'IP Address off CMTS derived from the file name',
   HDP_INSERT_TS                        TIMESTAMP   COMMENT     'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS                        TIMESTAMP   COMMENT     'Timestamp when the record was updated in Hive',
   HDP_FILE_NAME                        STRING      COMMENT     'Source file name from which the record was extracted',
   JOB_EXECUTION_ID                     STRING      COMMENT     'Job Execution ID maps to execution ID of loadcontrol'
)
PARTITIONED BY (PROCESSED_TS TIMESTAMP)
STORED AS ORC;
