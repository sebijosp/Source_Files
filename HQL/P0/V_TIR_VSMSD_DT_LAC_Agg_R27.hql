-- show view cdr_prod_view.V_TIR_VSMSD_DT_LAC_Agg_R27;
DROP VIEW IF EXISTS cdrdm.V_TIR_VSMSD_DT_LAC_Agg_R27;
CREATE VIEW cdrdm.V_TIR_VSMSD_DT_LAC_Agg_R27 AS
SELECT Udate, Subscriber_no, LAC, Description,  SUM(Call_Count) Call_Count,
          SUM(Minutes) Minutes, 
          SUM(SMS_Count) SMS_Count,
          SUM(W4G) W4G,
          SUM(W3G) W3G,
          SUM(W2G) W2G,
         SUM(WOther) WOther

FROM (

SELECT Udate, Utype, Subscriber_no, LAC, Description, Call_Count,
          Minutes, SMS_Count,
          CAST(0 AS DECIMAL(30,8)) AS  W4G,
          CAST(0 AS DECIMAL(30,8)) AS  W3G,
          CAST(0 AS DECIMAL(30,8)) AS  W2G,
          CAST(0 AS DECIMAL(30,8)) AS  WOther
          
FROM cdrdm.V_TIR_sum_VSMS_LAC 

UNION ALL

SELECT Udate, Utype, Subscriber_no, LAC, Description, CAST(0 AS INT) AS Call_Count,
          CAST(0 AS INT) AS Minutes, 
          CAST(0 AS INT) AS SMS_Count,
          W4G, W3G, W2G, WOther
FROM cdrdm.V_TIR_sum_Data_LAC
) X
GROUP BY Udate, Subscriber_no, LAC, Description;