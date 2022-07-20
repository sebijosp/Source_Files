-- show view cdr_prod_view.V_TIR_sum_VSMS_LAC;
DROP VIEW IF EXISTS cdrdm.V_TIR_sum_VSMS_LAC;
CREATE VIEW cdrdm.V_TIR_sum_VSMS_LAC AS 
SELECT Udate, 'Call-SMS' AS Utype, Subscriber_no, LAC, Description,
          SUM(call_count) Call_Count, 
          SUM(Minutes) Minutes,
          SUM(SMS_Count) SMS_Count
FROM
(
SELECT 
       --CAST (call_timestamp  AS DATE FORMAT 'dd/mm/yyyy' ) UDate, 
       --FROM_UNIXTIME(UNIX_TIMESTAMP(call_timestamp, 'dd/mm/yyyy'), 'yyyy-MM-dd') AS UDate,
       --to_date(call_timestamp) as UDate,
       call_timestamp_date as UDate,
	     subscriber_no, call_type, SUBSTR(CID,7,4) LAC,
       COALESCE(CASE WHEN call_type IN (2,5) THEN 1 ELSE 0 END,0) AS call_count,
       CAST(COALESCE(chargeable_duration,0)/60 AS INT) +
                               (CASE WHEN PMOD(COALESCE(chargeable_duration,0), 60) > 0 
                       THEN 1 
                       ELSE 0 
                  END) Minutes,
           COALESCE(CASE WHEN call_type IN (6,8) THEN 1 ELSE 0 END,0) AS SMS_count
FROM cdrdm.Fact_gsm_cdr_ext_Tbay
WHERE call_type IN (2,5,6,8) 
AND  LAC_TAC_Type ='R' 
AND IMSI_Type = 'T'
) RP27DTCTN 
LEFT OUTER JOIN ela_v21.V_MPS_CONFIG_DETAILS_GG 
ON LAC = hex_inp_val_str
GROUP BY Udate, subscriber_no, LAC, Description;
