CREATE TABLE HEM.IPDR_S07(      
   CMTS_HOST_NAME            STRING     COMMENT   'Contains the Fully Qualified Domain Name (FQDN) of the CMTS. If the CMTS does not have a domain name, it contains an empty string.',
   CMTS_SYS_UP_TIME          BIGINT     COMMENT   'Contains a 32-bit count of hundredths of a second since system initialization, in decimal notation.',
   CMTS_MD_IF_NAME           STRING     COMMENT   'Contains the first 50 characters of the ifName from the Interfaces Group MIB for the row entry corresponding to the CMTS Mac Domain interface (ifType = 127).',
   CMTS_MD_IF_INDEX          BIGINT     COMMENT   'Contains the ifIndex for the CMTS MAC domain interface (described in CmtsMdIfName).',
   CMTS_MD_CM_SG_ID          BIGINT     COMMENT   'Contains the ID of the MAC Domain Cable Modem ServiceGroup Id (MD-CM-SG-ID) in which the cable modem is registered.If the ID is unknown, the CMTS reports a value of zero.',
   CMTS_RCP_ID               STRING     COMMENT   'Contains the RCP-ID associated with the CM. If unknown, the CMTS returns a zero length hexBinary.',
   CMTS_RCC_STATUS_ID        BIGINT     COMMENT   'Contains the RCC id the CMTS used to configure the CM receive channel set during registration.If unknown, the CMTS returns 0.',
   CMTS_RCS_ID               INT        COMMENT   'Contains the Receive Channel Set (RCS) that the CM is currently using. If unknown, the CMTS returns the value zero.',
   CMTS_TCS_ID               INT        COMMENT   'Contains the Transmit Channel Set (TCS) that the CM is currently using.If unknown, the CMTS returns the value zero.',
   CM_MAC_ADDR               STRING     COMMENT   'Contains the MAC Address of the CM. If the CM has multiple MAC Addresses, it contains the MAC address associated with the Cable (i.e., RF MAC) interface. When the record is a Group Service Flow, the MAC address should contain a multicast address compliant with either [RFC 2464] or [RFC 1112] as applicable',
   CM_IPV4_ADDR              STRING     COMMENT   'Contains the IPv4 address of the CM.If the CM IPv4 address is unassigned or unknown, itcontains an empty string. If the CM has multiple IPv4 addresses, it contains the IPv4 address associated with theCable (i.e., RF MAC) interface.',
   CM_IPV6_ADDR              STRING     COMMENT   'Contains the IPv6 address of the CM.If the CM IPv6 address is unassigned or unknown, itcontains an empty string.',
   CM_IPV6_LINK_LOCAL_ADDR   STRING     COMMENT   'Contains the IPv6 Link Local address ofthe CM. If the CM IPv6 Link Local address is unassigned or unknown,it contains an empty string.',
   CM_QOS_VERSION            INT        COMMENT   'This attribute denotes the queuing services the CMregistered, either DOCSIS 1.1 QoS or DOCSIS 1.0 CoS mode.',
   CM_REG_STATUS_VALUE       INT        COMMENT   'Contains the current Cable Modem connectivity state,as specified in the OSSI Specification.Returned status information is the CM status as assumed by the CMTS.',
   CM_LAST_REG_TIME          BIGINT     COMMENT   'Contains the date and time value whenthe CM was last registered.',
   REC_TYPE                  INT        COMMENT   '1 - Interim (Service Flow is still running). 2- Stop (Completed), 3-Start , 4- Event (Single message record containing all the information)',
   REC_CREATION_TIME         BIGINT     COMMENT   'Contains a 64-bit count of milliseconds UTC time stamp at the time the data for the record was acquired.',
   CMTS_IP_ADDRESS           STRING     COMMENT   'IP Address off CMTS derived from the file name',
   HDP_INSERT_TS             TIMESTAMP  COMMENT   'Timestamp when the record was inserted into Hive',
   HDP_UPDATE_TS             TIMESTAMP  COMMENT   'Timestamp when the record was updated in Hive',
   HDP_FILE_NAME             STRING     COMMENT   'Source file name from which the record was extracted',
   JOB_EXECUTION_ID          STRING     COMMENT   'Job Execution ID maps to execution ID of loadcontrol'
)
PARTITIONED BY (PROCESSED_TS TIMESTAMP) 
STORED AS ORC;
