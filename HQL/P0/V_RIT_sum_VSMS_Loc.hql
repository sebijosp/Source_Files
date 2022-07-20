DROP VIEW IF EXISTS cdrdm.V_RIT_sum_VSMS_Loc;
CREATE VIEW cdrdm.V_RIT_sum_VSMS_Loc AS 
SELECT 
Udate, 
'Call-SMS' AS Utype, 
Subscriber_no, 
Cell_ID_comp, 
Route_Desc,
SUM(call_count) AS Call_Count, 
SUM(Duration_Sec) AS Sum_Dur_Sec, 
--SUM(Duration_Sec)/(NVL(SUM(call_count),1)*1.0000) AS Avg_Dur_Sec, -- 
SUM(Duration_Sec)/(if(SUM(call_count) is null OR SUM(call_count) = 0,1, SUM(call_count))*1.0000) AS Avg_Dur_Sec,
SUM(SMS_Count) AS SMS_Count, 
SUM(SMS_OG) AS SMS_OG_Cnt, 
SUM(SMS_IC) AS SMS_IN_Cnt
FROM

(SELECT
     --FROM_UNIXTIME(UNIX_TIMESTAMP(call_timestamp, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS UDate,
     call_timestamp_date AS UDate,
     subscriber_no, 
     call_type,
     CID,
     first_cell_id_extension,
     CONCAT(TRIM(COALESCE(first_cell_id_extension,TRIM(first_cell_id_extension))), SUBSTR(first_cell_id,11)) AS eCID,
     CONCAT(SUBSTR('00000000',1,8-LENGTH(TRIM(CONCAT(TRIM(COALESCE(first_cell_id_extension,TRIM(first_cell_id_extension))), SUBSTR(first_cell_id,11))))), CONCAT(TRIM(COALESCE(first_cell_id_extension,TRIM(first_cell_id_extension))), SUBSTR(first_cell_id,11))) AS Cell_ID_comp,
     COALESCE(CASE WHEN call_type IN (2,5) THEN 1 ELSE 0 END,0) AS call_count,
     COALESCE(Chargeable_duration,0) Duration_Sec,
     COALESCE(CASE WHEN call_type IN (6,8) THEN 1 ELSE 0 END,0) AS SMS_count,
     COALESCE(CASE WHEN call_type = 6  THEN 1 ELSE 0 END,0) AS SMS_OG,
     COALESCE(CASE WHEN call_type = 8  THEN 1 ELSE 0 END,0) AS SMS_IC
FROM cdrdm.Fact_gsm_cdr_ext_Tbay
WHERE call_type IN (2,5,6,8) 
AND  LAC_TAC_Type ='T' 
AND IMSI_Type = 'R'
) RP25DTCTN 
LEFT OUTER JOIN ela_v21.route_gg 
ON lpad(conv(ROUTE_ID,10,16),8,'0') = Cell_ID_comp
GROUP BY Udate,'Call-SMS',Subscriber_no,Cell_ID_comp,Route_Desc;