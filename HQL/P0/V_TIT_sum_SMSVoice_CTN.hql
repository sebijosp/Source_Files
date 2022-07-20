-- show view cdr_prod_view.V_TIT_sum_SMSVoice_CTN;
DROP VIEW IF EXISTS cdrdm.V_TIT_sum_SMSVoice_CTN;
CREATE VIEW cdrdm.V_TIT_sum_SMSVoice_CTN AS
SELECT UDate, subscriber_no, UType, 
SUM(CAST(COALESCE(chargeable_duration,0)/60 AS INT) 
       + (CASE WHEN PMOD(COALESCE(chargeable_duration,0), 60) > 0 
                       THEN 1 
                       ELSE 0 
                  END)) AS Minutes,
				 COUNT(*) AS cnt FROM 
(SELECT 
       --to_date(call_timestamp) AS UDate,
       call_timestamp_date AS UDate,
       subscriber_no, 
       CASE WHEN call_type IN (2,5) THEN 'CALL' ELSE 'SMS' END AS UType,
	   chargeable_duration
        
FROM cdrdm.Fact_gsm_cdr_ext_Tbay 
WHERE call_type IN (2,5,6,8) 
AND  LAC_TAC_Type ='T' 
AND IMSI_Type = 'T' ) X
GROUP BY UDate, subscriber_no, UType;