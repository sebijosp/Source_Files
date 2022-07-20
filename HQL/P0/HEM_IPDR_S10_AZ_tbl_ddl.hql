CREATE TABLE hem.ipdr_s10_az(                                    
  cmts_host_name string COMMENT 'Contains the Fully Qualified Domain Name (FQDN) of the CMTS. If the CMTS does not have a domain name, it contains an empty string.',                                                                                         
  cmts_sys_up_time bigint COMMENT 'Contains a 32-bit count of hundredths of a second since system initialization, in decimal notation.',                                                                                                                      
  cmts_md_if_name string COMMENT 'Contains the first 50 characters of the ifName from the Interfaces Group MIB for the row entry corresponding to the CMTS Mac Domain interface (ifType = 127).',                                                             
  cmts_md_if_index bigint COMMENT 'Contains the ifIndex for the CMTS MAC domain interface (described in CmtsMdIfName).',                                                                                                                                      
  cm_mac_addr string COMMENT 'Contains the MAC Address of the CM. If the CM has multiple MAC Addresses, it contains the MAC address associated with the Cable (i.e., RF MAC) interface. When the record is a Group Service Flow, the MAC address should contain a multicast address compliant ',  
  cm_reg_status_id bigint COMMENT 'Contains the id value to uniquely identify a CM.',                                                                                                                                                                         
  cmts_cm_us_ch_if_name string COMMENT 'Contains the first 50 characters of the ifName from the Interfaces Group MIB for the row entry corresponding to the CMTS upstream interface (ifType = 129)',                                                          
  cmts_cm_us_ch_if_index bigint COMMENT 'Contains the ifIndex for the upstream interface (described in CmtsCmUsChIfName)',                                                                                                                                    
  cmts_cm_us_ch_id int COMMENT 'Contains the US channel id as defined in docsIf3MdChCfgChId',                                                                                                                                                                 
  cmts_cm_us_modulation_type int COMMENT 'Contains the modulation type currently used by this upstream channel.',                                                                                                                                             
  cmts_cm_us_rx_power int COMMENT 'Contains the receive power as perceived for the upstream channel.',                                                                                                                                                        
  cmts_cm_us_signal_noise int COMMENT 'Contains Signal/Noise ratio as perceived for upstream data from the CM.',                                                                                                                                              
  cmts_cm_us_microreflections int COMMENT 'Contains the microreflections received on this interface. ',                                                                                                                                                       
  cmts_cm_us_eq_data string COMMENT 'Contains the equalization data for the CM. ',                                                                                                                                                                            
  cmts_cm_us_unerroreds bigint COMMENT 'Contains the codewords received without error from the CM.',                                                                                                                                                          
  cmts_cm_us_correcteds int COMMENT ' Contains codewords received with correctable errors from the CM.',                                                                                                                                                      
  cmts_cm_us_uncorrectables int COMMENT 'Contains codewords received with uncorrectable errors from the CM.',                                                                                                                                                 
  cmts_cm_us_high_resolution_timing_offset int COMMENT 'Contains the higher resolution timing offset to provide a finer granularity timing offset.',                                                                                                          
  cmts_cm_us_is_muted int COMMENT 'Denotes if the CM upstream channel has been muted via CM-CTRL-REQ or CM-CTRL-RSP message exchange.',                                                                                                                       
  cmts_cm_us_ranging_status int COMMENT 'Contains the ranging status of the CM',                                                                                                                                                                              
  rec_type int COMMENT '1 - Interim (Service Flow is still running). 2- Stop (Completed), 3-Start , 4- Event (Single message record containing all the information)',                                                                                         
  cmts_ip_address string COMMENT 'IP Address off CMTS derived from the file name',                                                                                                                                                                            
  hdp_insert_ts timestamp COMMENT 'Timestamp when the record was inserted into Hive',                                                                                                                                                                         
  hdp_update_ts timestamp COMMENT 'Timestamp when the record was updated in Hive',                                                                                                                                                                            
  hdp_file_name string COMMENT 'Source file name from which the record was extracted',                                                                                                                                                                        
  job_execution_id string COMMENT 'Job Execution ID maps to execution ID of loadcontrol',
  az_insert_ts timestamp,   
  az_update_ts timestamp,
  exec_run_id STRING
)   
PARTITIONED BY (processed_ts timestamp)  
STORED AS ORC;  

