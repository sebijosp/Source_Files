CREATE TABLE hem.ipdr_s24_az(                                    
    cmts_host_name string COMMENT 'CMST NAME',
    cmts_ds_if_index bigint COMMENT 'The ifIndex from the Interfaces Group MIB for the CMTS downstream interface',
    profile_id INT COMMENT 'PROFILE ID OF downstream OFDM profiles or upstream OFDMA profiles/Data IUCs',
    fullchannelspeed bigint COMMENT 'FULL CHANNEL SPEED',
    outoctets bigint COMMENT 'This attribute is the count of MAC-layer octets transmitted by the CCAP using this profile. This value is the size of all unicast, multicast or broadcast frames (including all MAC-layer framing) delivered from the MAC to the PHY - this includes user data, ',
    outunicastoctets bigint COMMENT 'his attribute is the count of MAC-layer Unicast octets transmitted by the CCAP using this profile. This value is the size of all unicast frames (including all MAC-layer framing) delivered from the MAC to the PHY - this includes user data, DOCSIS MAC Manage',
    outmulticastoctets bigint COMMENT 'This attribute is the count of multicast and broadcast octets transmitted by the CCAP using this profile. This value is the size of all frames (including all MAC-layer framing) delivered from the MAC to the PHY and addressed to a multicast MAC address - th',
    outframes bigint COMMENT 'This attribute is the count of frames transmitted by the CCAP using this profile. This value is the count of all unicast, multicast or broadcast frames delivered from the MAC to the PHY - this includes user data, DOCSIS MAC Management Messages, etc. Discon',
    outunicastframes bigint COMMENT 'This attribute is the count of unicast frames transmitted by the CCAP using this profile. This value is the count of all frames delivered from the MAC to the PHY and addressed to a unicast MAC address - this includes user data, DOCSIS MAC Management Messag',
    outmulticastframes bigint COMMENT 'This attribute is the count of multicast frames transmitted by the CCAP using this profile. This value is the count of all frames delivered from the MAC to the PHY and addressed to a multicast MAC address - this includes user data, DOCSIS MAC Management Me',
    counterdiscontinuitytime bigint COMMENT 'This attribute is the value of sysUpTime on the most recent occasion at which any one or more of this entrys counters suffered a discontinuity. If no such discontinuities have occurred since the last re-initialization of the local management subsystem, the',
    rectype INT COMMENT 'typical ipdr record type',
    reccreationtime bigint COMMENT 'RecCreationTime',
    cmts_ip_address string COMMENT 'IP Address off CMTS derived from the file name',
    hdp_insert_ts TIMESTAMP COMMENT 'Timestamp when the record was inserted into Hive',
    hdp_update_ts TIMESTAMP COMMENT 'Timestamp when the record was updated in Hive',
    hdp_file_name string COMMENT 'Source file name from which the record was extracted',
    job_execution_id string COMMENT 'Job Execution ID maps to execution ID of loadcontrol',
    az_insert_ts TIMESTAMP COMMENT 'ETL insert time to azure',
    az_update_ts TIMESTAMP COMMENT 'ETL update ts to azure',
    exec_run_id STRING COMMENT 'ETL exec run id ie pipeline runid'
)   
PARTITIONED BY (processed_ts timestamp)  
STORED AS ORC;  

