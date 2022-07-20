DROP TABLE HEM.CCAP_US_PORT_UTIL_15_FCT;
CREATE TABLE HEM.CCAP_US_PORT_UTIL_15_FCT
(
Network_Topology_Key                 BIGINT    comment 'Network Topology Key',
Cmts_Host_Name                       STRING    COMMENT 'Fully Qualified Domain Name (FQDN) of the CMTS. If the CMTS does not have a domain name  it contains an empty string.',
Cmts_Md_If_Index                     BIGINT    COMMENT 'Contains the ifIndex for the CMTS MAC domain interface (described in CmtsMdIfName).',
Us_port                              STRING    COMMENT 'UP Stream Port.',
Us_Util_Interval                     INT       COMMENT 'Time interval over which the channel utilization index is calculated.(sec)',
Interface_Count                      BIGINT    comment 'Count of the Interface',
Is_port_lvl_flg                      STRING    COMMENT  'Flag indicates S-SCQAM,O-OFDMA , P=Port',
Us_Util_Index_Percentage             DECIMAL(10,2)       COMMENT 'Calculated and truncated utilization index percentage for the downstream interface.',
Us_Util_Total_Mslots                 BIGINT    COMMENT 'Count of mini-slots defined for US logical channel interface from CMTS initialization. Includes all IUCs, SIDs and NULL SID for inactive logical channel.',
Us_Util_UcastGranted_Mslots          BIGINT    COMMENT 'Count of unicast granted mini-slots on the US logical channel regardless of burst type. Unicast granted mini-slots are those in which the CMTS assigned bandwidth to any unicast SID on the logical channel. However, this object does not include mini-slots for reserved IUCs, or grants to SIDs designated as meaning no CM.',
Us_Util_Total_Cntn_Mslots            BIGINT    COMMENT 'Count, from CMTS initialization, of contention mini-slots defined for this upstream logical channel. Includes all mini-slots assigned to a broadcast or multicast SID on the logical channel.',
Us_Util_Used_Cntn_Mslots             BIGINT    COMMENT 'Count, from CMTS initialization, of contention mini-slots utilized on the upstream logical channel. For contention regions, utilized mini-slots are those in which the CMTS correctly received an upstream burst from any CM on the upstream logical channel.',
Us_Util_Coll_Cntn_Mslots             BIGINT    COMMENT 'Count, from CMTS initialization, of collision contention mini-slots on the upstream logical channel. For contention regions, these are the mini-slots applicable to burst that the CMTS detected but could not correctly receive.',
Us_Util_Total_Cntn_Req_Mslots        BIGINT    COMMENT 'Count, from CMTS initialization, of contention request mini-slots defined for this upstream logical channel. Includes all mini-slots for IUC1 assigned to a broadcast or multicast SID on the logical channel.',
Us_Util_Used_Cntn_Req_Mslots         BIGINT    COMMENT 'Count, from CMTS initialization, of contention request mini-slots utilized on this upstream logical channel. Includes all contention mini-slots for UIC1 applicable to bursts that the CMTS correctly received.',
Us_Util_Coll_Cntn_Req_Mslots         BIGINT    COMMENT 'Count, from CMTS initialization, of contention request mini-slots subjected to collisions on this upstream logical channel. This includes all contention mini-slots for IUC1 applicable to bursts that the CMTS detected but could not correctly receive.',
Us_Util_Total_Cntn_Req_Data_Mslots   BIGINT    COMMENT 'Count, from CMTS initialization, of contention request data mini-slots defined for this upstream logical channel. Includes all mini-slots for IUC2 assigned to a broadcast or multicast SID on the logical channel.',
Us_Util_Used_Cntn_Req_Data_Mslots    BIGINT    COMMENT 'Count, from CMTS initialization, of contention request data mini-slots utilized on this upstream logical channel. This includes all contention mini-slots for IUC2 applicable to bursts that the CMTS correctly received. Count, from CMTS initialization, of contention request data mini-slots defined for this upstream logical channel. Includes all mini-slots for IUC2 assigned to a broadcast or multicast SID on the logical channel.',
Us_Util_Coll_Cntn_Req_Data_Mslots    BIGINT    COMMENT 'Count, from CMTS initialization, of contention request data mini-slots subjected to collisions on this upstream logical channel. This includes all contention mini-slots for IUC2 applicable bursts that the CMTS detected but could not correctly receive.',
Us_Util_Total_Cntn_Init_Maint_Mslots BIGINT    COMMENT 'Count, from CMTS initialization, of initial maintenance mini-slots defined for this upstream logical channel. Includes all mini-slots for IUC3 assigned to a broadcast or multicast SID on the logical channel.',
Us_Util_Used_Cntn_Init_Maint_Mslots  BIGINT    COMMENT 'Count, from CMTS initialization, of initial maintenance mini-slots utilized on this upstream logical channel. This includes all contention mini-slots for IUC3 applicable to bursts that the CMTS correctly received.',
Us_Util_Coll_Cntn_Init_Maint_Mslots  BIGINT    COMMENT 'Count, from CMTS initialization, of contention initial maintenance mini-slots subjected to collisions on this upstream logical channel. This includes all contention mini-slots for IUC3 applicable to bursts that the CMTS detected but could not correctly receive.',
capacity_bps                         BIGINT    COMMENT 'No of times port congested in a day ',
us_throughput_bps                    BIGINT    COMMENT 'No of times port congested in a day ',
us_bytes                             BIGINT    COMMENT 'No of times port congested in a day ',
rop                                  BIGINT    COMMENT 'No of times port congested in a day ',
is_pt                                INT       COMMENT 'Flag indicates prime time',
is_fullday                           INT       COMMENT  'Flag indicates full day',
HDP_INSERT_TS                        TIMESTAMP COMMENT 'Timestamp when the record was inserted into Hive',
HDP_UPDATE_TS                        TIMESTAMP COMMENT 'Timestamp when the record was updated in Hive',
JOB_EXECUTION_ID                     STRING    COMMENT 'Job Execution ID maps to execution ID of loadcontrol'

) PARTITIONED BY (Date_Key TIMESTAMP)
STORED AS ORC;
