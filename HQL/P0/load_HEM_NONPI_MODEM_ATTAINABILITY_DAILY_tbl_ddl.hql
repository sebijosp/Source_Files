INSERT OVERWRITE TABLE HEM.NONPI_MODEM_ATTAINABILITY_DAILY PARTITION(EVENT_DATE)
SELECT 
CM_MAC_ADDR
,CMTS_HOST_NAME
,CMTS_MD_IF_INDEX
,CMTS_MD_IF_NAME
,MODEM_MAKE
,MODEM_MODEL
,PRODUCT_CODE
,TIER
,TECHNOLOGY
,DS_TUNERS_COUNT
,US_TUNERS_COUNT
,DOWNLOAD_SPEED_KBPS
,UPLOAD_SPEED_KBPS
,DOWNLOAD_SPEED_MBPS
,UPLOAD_SPEED_MBPS
,UP_MBYTES
,DOWN_MBYTES
,TOTAL_USG_MBYTES
,COUNT_ACTUAL_DS_IMPACT_FULL     
,COUNT_ACTUAL_DSUS_IMPACT_FULL 
,COUNT_ACTUAL_US_IMPACT_FULL   
,TOTAL_COUNTERS_FULL         
,COUNT_ACTUAL_DS_IMPACT_ALL
,COUNT_ACTUAL_DSUS_IMPACT_ALL  
,COUNT_ACTUAL_US_IMPACT_ALL
,TOTAL_COUNTERS_ALL
,COUNT_ACTUAL_DS_IMPACT_PRIME
,COUNT_ACTUAL_DSUS_IMPACT_PRIME
,COUNT_ACTUAL_US_IMPACT_PRIME
,TOTAL_COUNTERS_PRIME
,DS_SPEED_ATTAINABLE_FULL
,DS_SPEED_ATTAINABLE_ALL
,DS_SPEED_ATTAINABLE_PRIME
,US_SPEED_ATTAINABLE_FULL
,US_SPEED_ATTAINABLE_ALL
,US_SPEED_ATTAINABLE_PRIME
,POTENTIAL_IMPACT_DS
,ACTUAL_IMPACT_DS
,POTENTIAL_IMPACT_DSUS
,ACTUAL_IMPACT_DSUS
,POTENTIAL_IMPACT_US
,ACTUAL_IMPACT_US
,POTENTIAL_IMPACT
,ACTUAL_IMPACT
,ACTUAL_IMPACT_COUNTER
,TTL
,ATTAINABILITY_PCT
,CBU_CODE
,TOKENIZE(FULL_ACCT_NUM,'K1') AS FULL_ACCOUNT_NUMBER
,TOKENIZE(ACCOUNT,'K1') AS ACCOUNT
,DTV_MODEL_NUM
,DTV_SERIAL_NUM
,SYSTEM
,PRIMARY_HUB
,SECONDARY_HUB
,SMT
,MUNICIPALITY
,EQUIPMENT_STATUS_CODE
,BILLING_SRC_MAC_ID
,POSTAL_ZIP_CODE
,NODE
,RTN_SEG
,CLAMSHELL
,FIRMWARE
,BB_TIER
,HH_TYPE
,CONTRACT_TYPE
,TIMEZONE
,IS_DST
,HDP_INSERT_TS
,HDP_UPDATE_TS
,COUNT_NW_ACTUAL_DS_IMPACT_FULL
,COUNT_NW_ACTUAL_DSUS_IMPACT_FULL
,COUNT_NW_ACTUAL_US_IMPACT_FULL   
,TOTAL_COUNTERS_NW_FULL  
,COUNT_NW_ACTUAL_DS_IMPACT_ALL
,COUNT_NW_ACTUAL_DSUS_IMPACT_ALL  
,COUNT_NW_ACTUAL_US_IMPACT_ALL
,TOTAL_COUNTERS_NW_ALL
,COUNT_NW_ACTUAL_DS_IMPACT_PRIME
,COUNT_NW_ACTUAL_DSUS_IMPACT_PRIME
,COUNT_NW_ACTUAL_US_IMPACT_PRIME
,TOTAL_COUNTERS_NW_PRIME
,ACTUAL_IMPACT_COUNTER_FULL
,ACTUAL_IMPACT_COUNTER_PRIME
,PORT_GROUP
,EVENT_DATE
FROM 
HEM.MODEM_ATTAINABILITY_DAILY
--WHERE EVENT_DATE = '2020-05-01'
WHERE EVENT_DATE>='${hiveconf:DeltaPartStart}' 
;
