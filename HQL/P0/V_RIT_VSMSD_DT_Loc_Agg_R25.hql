DROP VIEW IF EXISTS cdrdm.V_RIT_VSMSD_DT_Loc_Agg_R25;
CREATE VIEW cdrdm.V_RIT_VSMSD_DT_Loc_Agg_R25 AS
SELECT 
Udate, 
Subscriber_no, 
Cell_ID_comp, 
Route_Desc,  
SUM(Call_Count) Call_Count,
SUM(Sum_Dur_Sec) Sum_Dur_Sec, 
--SUM(Sum_Dur_Sec)/(NVL(SUM(call_count),1)*1.0000) Avg_Dur_Sec, -- 
SUM(Sum_Dur_Sec)/(if(SUM(call_count) = 0,1, SUM(call_count))*1.0000) AS Avg_Dur_Sec,
SUM(SMS_Count) SMS_Count, 
SUM(SMS_OG_Cnt) SMS_OG_Cnt, SUM(SMS_IN_Cnt) SMS_IN_Cnt,
SUM(Data_Usage_MB) Data_Usage_MB
FROM (
SELECT 
     Udate, 
     Utype, 
     Subscriber_no, 
     Cell_ID_comp, 
     Route_Desc, 
     Call_Count,
     Sum_Dur_Sec, 
     Avg_Dur_Sec, 
     SMS_Count, 
     SMS_OG_Cnt, 
     SMS_IN_Cnt,
     CAST(0 AS DECIMAL(30,8)) AS Data_Usage_MB
FROM cdrdm.V_RIT_sum_VSMS_Loc 

UNION ALL

SELECT 
     Udate, 
     Utype, 
     Subscriber_no, 
     Cell_ID_comp, 
     Route_Desc, 
     CAST(0 AS INT) AS Call_Count,
     CAST(0 AS INT) AS Sum_Dur_Sec, 
     CAST(0 AS DECIMAL(15,5)) AS Avg_Dur_Sec, 
     CAST(0 AS INT) AS SMS_Count, 
     CAST(0 AS INT)  AS SMS_OG_Cnt, 
     CAST(0 AS INT) AS SMS_IN_Cnt,
     Data_Usage_MB
FROM cdrdm.V_RIT_sum_Data_loc 

) X
GROUP BY Udate,Subscriber_no,Cell_ID_comp,Route_Desc;