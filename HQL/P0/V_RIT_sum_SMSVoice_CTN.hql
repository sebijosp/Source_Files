DROP VIEW IF EXISTS cdrdm.V_RIT_sum_SMSVoice_CTN;
CREATE VIEW cdrdm.V_RIT_sum_SMSVoice_CTN AS
SELECT
call_timestamp_date AS UDate,
subscriber_no, 
CASE WHEN call_type IN (2,5) THEN 'CALL' ELSE 'SMS' END AS UType, 
CID,
SUM(CAST(COALESCE(chargeable_duration,0)/60 AS INT) +(CASE WHEN PMOD(COALESCE(chargeable_duration,0), 60) > 0 THEN 1 ELSE 0 END)) AS Minutes,
COUNT(*) AS Cnt
FROM cdrdm.Fact_gsm_cdr_ext_Tbay
WHERE call_type IN (2,5,6,8) AND  LAC_TAC_Type ='T' AND IMSI_Type = 'R' 
GROUP BY call_timestamp_date,subscriber_no,call_type,CID;