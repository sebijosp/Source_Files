!echo ;
!echo 

DROP TABLE IF EXISTS HEM.IPDR_S24;
CREATE TABLE HEM.IPDR_S24(
    CMTS_HOST_NAME          STRING      COMMENT 'CMTS host NAME',
    CMTS_DS_IF_INDEX        BIGINT      COMMENT 'The ifIndex from the Interfaces Group MIB for the CMTS downstream interface',
    Profile_Id              INT	        COMMENT 'PROFILE ID OF downstream OFDM profiles or upstream OFDMA profiles/Data IUCs',
    FullChannelSpeed        BIGINT	    COMMENT 'FULL CHANNEL SPEED',
    OutOctets               BIGINT	    COMMENT 'This attribute is the count of MAC-layer octets transmitted by the CCAP using this profile. This value is the size of all unicast, multicast or broadcast frames (including all MAC-layer framing) delivered from the MAC to the PHY - this includes user data, DOCSIS MAC Management Messages, etc. Discontinuities in the value of this counter can occur at reinitialization of the managed system, and at other times as indicated by the value of ProfileCounterDiscontinuityTime',
    OutUnicastOctets        BIGINT	    COMMENT 'his attribute is the count of MAC-layer Unicast octets transmitted by the CCAP using this profile. This value is the size of all unicast frames (including all MAC-layer framing) delivered from the MAC to the PHY - this includes user data, DOCSIS MAC Management Messages, etc. Discontinuities in the value of this counter can occur at reinitialization of the managed system, and at other times as indicated by the value of ProfileCounterDiscontinuityTime',
    OutMulticastOctets      BIGINT	    COMMENT 'This attribute is the count of multicast and broadcast octets transmitted by the CCAP using this profile. This value is the size of all frames (including all MAC-layer framing) delivered from the MAC to the PHY and addressed to a multicast MAC address - this includes user data, DOCSIS MAC Management Messages, etc. Discontinuities in the value of this counter can occur at reinitialization of the managed system, and at other times as indicated by the value of ProfileCounterDiscontinuityTime',
    OutFrames               BIGINT	    COMMENT 'This attribute is the count of frames transmitted by the CCAP using this profile. This value is the count of all unicast, multicast or broadcast frames delivered from the MAC to the PHY - this includes user data, DOCSIS MAC Management Messages, etc. Discontinuities in the value of this counter can occur at reinitialization of the managed system, and at other times as indicated by the value of ProfileCounterDiscontinuityTime',
    OutUnicastFrames        BIGINT      COMMENT 'This attribute is the count of unicast frames transmitted by the CCAP using this profile. This value is the count of all frames delivered from the MAC to the PHY and addressed to a unicast MAC address - this includes user data, DOCSIS MAC Management Messages, etc. Discontinuities in the value of this counter can occur at reinitialization of the managed system, and at other times as indicated by the value of ProfileCounterDiscontinuityTime',
    OutMulticastFrames      BIGINT      COMMENT 'This attribute is the count of multicast frames transmitted by the CCAP using this profile. This value is the count of all frames delivered from the MAC to the PHY and addressed to a multicast MAC address - this includes user data, DOCSIS MAC Management Messages, etc., but excludes frames sent to a broadcast address. Discontinuities in the value of this counter can occur at reinitialization of the managed system, and at other times as indicated by the value of ProfileCounterDiscontinuityTime',
   CounterDiscontinuityTime BIGINT      COMMENT 'This attribute is the value of sysUpTime on the most recent occasion at which any one or more of this entrys counters suffered a discontinuity. If no such discontinuities have occurred since the last re-initialization of the local management subsystem, then this attribute contains a zero value',
   RecType                  INT         COMMENT 'typical ipdr record type',
   RecCreationTime          BIGINT      COMMENT 'RecCreationTime',
   CMTS_IP_ADDRESS           STRING     COMMENT   'IP Address off CMTS derived from the file name',
   HDP_INSERT_TS            TIMESTAMP   COMMENT 'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS            TIMESTAMP   COMMENT 'Timestamp when the record was updated in Hive',
   HDP_FILE_NAME            STRING      COMMENT 'Source file name from which the record was extracted',
   JOB_EXECUTION_ID         STRING      COMMENT 'Job Execution ID maps to execution ID of loadcontrol'
)
PARTITIONED BY (PROCESSED_TS TIMESTAMP)
STORED AS ORC;
