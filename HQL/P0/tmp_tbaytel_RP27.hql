set hive.cli.print.header=true;
SET hive.execution.engine=tez;

SELECT LAC, Description,  
          SUM(Call_Count) Call_Count,
          SUM(Minutes) Minutes, 
          SUM(SMS_Count) SMS_Count,
          SUM(W4G) W4G,
          SUM(W3G) W3G,
          SUM(W2G) W2G,
          SUM(WOther) WOther
FROM cdrdm.V_TIR_VSMSD_DT_LAC_Agg_R27
WHERE UDate BETWEEN '2022-05-01' AND '2022-05-31' 
GROUP BY LAC, Description;
