-- Need to check MOD function related condition/logic in actual script
DROP VIEW IF EXISTS cdrdm.V_Tbay_OB_DT_TAP_Agg_R29;
CREATE VIEW cdrdm.V_Tbay_OB_DT_TAP_Agg_R29 AS
SELECT UDate, PLMN, CAST(SUM(VOICE_Min) AS BIGINT) VOICE_Min, 
          CAST(SUM(SMS_Cnt) AS BIGINT) SMS_Cnt, 
          SUM(GPRS_MBytes) GPRS_MBytes
FROM 

       ( SELECT  --CAST(local_time_stamp AS DATE FORMAT 'dd/mm/yyyy' ) UDate, 
               -- FROM_UNIXTIME(UNIX_TIMESTAMP(local_time_stamp,'dd/mm/yyyy'),'yyyy-MM-dd') AS UDate,
               --to_date(local_time_stamp) AS UDate,
                   local_time_stamp_date AS UDate,
			             PLMN_TADIG_From_TAP_FILE PLMN,
                   (CASE WHEN 
                   (imsi BETWEEN 302720392000000 AND 302720392999999 OR -- HSPA
                   imsi BETWEEN 302720592000000 AND 302720592999999 OR -- LTE
                   imsi BETWEEN 302720692000000 AND 302720692999999)  -- LTE
                  THEN IMSI
                  ELSE 'NON-TBT'
                   END) IMSI_TYPE,
        
                   (CASE WHEN tele_service_code NOT IN ('20','21','22') AND record_type_ind IN ('MOC','MTC') 
                          THEN  CAST(CAST(total_call_event_duration AS BIGINT)/60 AS BIGINT) + 
                                -- (CASE WHEN (CAST(total_call_event_duration AS BIGINT)) MOD 60 > 0 THEN 1 ELSE 0 END) -- IF total_call_event_duration is always +ve then this logic is not accurate   
                                ( CASE WHEN PMOD(CAST(total_call_event_duration AS BIGINT), 60) > 0 THEN 1 ELSE 0 END ) -- PMOD always returns +ve value and hence this will always TRUE
                          ELSE 0 
                    END) AS VOICE_Min,
        
                   (CASE WHEN tele_service_code IN ('20','21','22') AND record_type_ind IN ('MOC','MTC') 
                          THEN 1 
                          ELSE 0 
                    END) AS SMS_Cnt,
                   
                   (CASE WHEN record_type_ind IN ('GPRS') 
                          THEN  (COALESCE(data_volume_incoming,0)+ COALESCE(data_volume_outgoing,0))/1048576.00000000
                          ELSE 0 
                    END) AS GPRS_MBytes
        FROM cdrdm.FACT_TAP3IN_CDR
        
) X WHERE   X.IMSI_TYPE <>  'NON-TBT' 
GROUP BY UDate, PLMN;