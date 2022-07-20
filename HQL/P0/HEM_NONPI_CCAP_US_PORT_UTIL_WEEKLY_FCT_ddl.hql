CREATE TABLE HEM.NONPI_CCAP_US_PORT_UTIL_WEEKLY_FCT(                                                                                                                                                                                                                
  week_id decimal(6,0) COMMENT 'Week id',                                                                                                                                                                                                                     
  network_topology_key bigint COMMENT 'Network Topology Key',                                                                                                                                                                                                 
  cmts_host_name string COMMENT 'Fully Qualified Domain Name (FQDN) of the CMTS. If the CMTS does not have a domain name it contains an empty string.',                                                                                                       
  cmts_md_if_index bigint COMMENT 'Interface Index for the CMTS MAC domain interface (described in CmtsMdIfName).',                                                                                                                                           
  us_port string COMMENT 'UP Stream Port.',                                                                                                                                                                                                                   
  us_util_interval int COMMENT 'Time interval over which the channel utilization index is calculated.(sec)',                                                                                                                                                  
  interface_count bigint COMMENT 'Count of the Interface',                                                                                                                                                                                                    
  is_port_lvl_flg string COMMENT 'Flag indicates S-SCQAM,O-OFDMA , P=Port',                                                                                                                                                                                   
  us_util_total_mslots bigint COMMENT 'Count of mini-slots defined for US logical channel interface from CMTS initialization. Includes all IUCs, SIDs and NULL SID for inactive logical channel.',                                                            
  us_util_ucastgranted_mslots bigint COMMENT 'Count of unicast granted mini-slots on the US logical channel regardless of burst type. Unicast granted mini-slots are those in which the CMTS assigned bandwidth to any unicast SID on the logical channel. However, this object does not include mini-slots fo',  
  us_util_total_cntn_mslots bigint COMMENT 'Count, from CMTS initialization, of contention mini-slots defined for this upstream logical channel. Includes all mini-slots assigned to a broadcast or multicast SID on the logical channel.',                   
  us_util_used_cntn_mslots bigint COMMENT 'Count, from CMTS initialization, of contention mini-slots utilized on the upstream logical channel. For contention regions, utilized mini-slots are those in which the CMTS correctly received an upstream burst from any CM on the upstream logical channel.',  
  us_util_coll_cntn_mslots bigint COMMENT 'Count, from CMTS initialization, of collision contention mini-slots on the upstream logical channel. For contention regions, these are the mini-slots applicable to burst that the CMTS detected but could not correctly receive.',  
  us_util_total_cntn_req_mslots bigint COMMENT 'Count, from CMTS initialization, of contention request mini-slots defined for this upstream logical channel. Includes all mini-slots for IUC1 assigned to a broadcast or multicast SID on the logical channel.',  
  us_util_used_cntn_req_mslots bigint COMMENT 'Count, from CMTS initialization, of contention request mini-slots utilized on this upstream logical channel. Includes all contention mini-slots for UIC1 applicable to bursts that the CMTS correctly received.',  
  us_util_coll_cntn_req_mslots bigint COMMENT 'Count, from CMTS initialization, of contention request mini-slots subjected to collisions on this upstream logical channel. This includes all contention mini-slots for IUC1 applicable to bursts that the CMTS detected but could not correctly receive.',  
  us_util_total_cntn_req_data_mslots bigint COMMENT 'Count, from CMTS initialization, of contention request data mini-slots defined for this upstream logical channel. Includes all mini-slots for IUC2 assigned to a broadcast or multicast SID on the logical channel.',  
  us_util_used_cntn_req_data_mslots bigint COMMENT 'Count, from CMTS initialization, of contention request data mini-slots utilized on this upstream logical channel. This includes all contention mini-slots for IUC2 applicable to bursts that the CMTS correctly received. Count, from CMTS initialization, of co',  
  us_util_coll_cntn_req_data_mslots bigint COMMENT 'Count, from CMTS initialization, of contention request data mini-slots subjected to collisions on this upstream logical channel. This includes all contention mini-slots for IUC2 applicable bursts that the CMTS detected but could not correctly receive.',  
  us_util_total_cntn_init_maint_mslots bigint COMMENT 'Count, from CMTS initialization, of initial maintenance mini-slots defined for this upstream logical channel. Includes all mini-slots for IUC3 assigned to a broadcast or multicast SID on the logical channel.',  
  us_util_used_cntn_init_maint_mslots bigint COMMENT 'Count, from CMTS initialization, of initial maintenance mini-slots utilized on this upstream logical channel. This includes all contention mini-slots for IUC3 applicable to bursts that the CMTS correctly received.',  
  us_util_coll_cntn_init_maint_mslots bigint COMMENT 'Count, from CMTS initialization, of contention initial maintenance mini-slots subjected to collisions on this upstream logical channel. This includes all contention mini-slots for IUC3 applicable to bursts that the CMTS detected but could not correctly rec',  
  utilization_max decimal(10,2) COMMENT 'Calculated and truncated utilization index percentage for the upstream interface.',                                                                                                                                  
  utilization_95 decimal(10,2) COMMENT 'Calculated and truncated utilization index percentage for the upstream interface.',                                                                                                                                   
  utilization_98 decimal(10,2) COMMENT 'Calculated and truncated utilization index percentage for the upstream interface.',                                                                                                                                   
  us_bytes bigint COMMENT 'Calculated upstream bytes for the interface.',                                                                                                                                                                                     
  capacity_bps bigint COMMENT 'Calculated capacity bps .',                                                                                                                                                                                                    
  us_throughput_bps bigint COMMENT 'Calculated and truncated throughput index percentage for the upstream interface.',                                                                                                                                        
  us_throughput_max bigint COMMENT 'Calculated and truncated throughput index percentage for the upstream interface.',                                                                                                                                        
  us_throughput_bps_95 bigint COMMENT 'Calculated and truncated throughput index percentage for the upstream interface.',                                                                                                                                     
  us_throughput_bps_98 bigint COMMENT 'Calculated and truncated throughput index percentage for the upstream interface.',                                                                                                                                     
  rop bigint COMMENT 'No of times port congested in a day ',                                                                                                                                                                                                  
  us_util_index_percentage decimal(10,2) COMMENT 'Calculated and truncated utilization index percentage for the upstream interface.',                                                                                                                         
  time_flag string COMMENT 'Filter query for is_pt for P and is_full_day for F')                                                                                                                                                             
PARTITIONED BY (date_key date)    
STORED AS ORC;  
