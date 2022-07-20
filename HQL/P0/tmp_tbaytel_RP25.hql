set hive.cli.print.header=true;
SET hive.execution.engine=tez;

SELECT 
Cell_ID_comp, 
Route_Desc,  
SUM(Call_Count) AS Call_Count,
SUM(Sum_Dur_Sec) AS Sum_Dur_Sec, 
SUM(Sum_Dur_Sec)/(NVL(SUM(call_count),0)*1.0000) AS Avg_Dur_Sec,
SUM(SMS_Count) AS SMS_Count, 
SUM(SMS_OG_Cnt) AS SMS_OG_Cnt, 
SUM(SMS_IN_Cnt) AS SMS_IN_Cnt,
SUM(Data_Usage_MB) AS Data_Usage_MB
FROM cdrdm.V_RIT_VSMSD_DT_Loc_Agg_R25
WHERE UDate BETWEEN '2022-05-01' AND '2022-05-31'
GROUP BY Cell_ID_comp,Route_Desc;
