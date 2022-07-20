set hive.cli.print.header=true;
SET hive.execution.engine=tez;

SELECT Subscriber_no, SUM(minutes) Minutes, 
       SUM(W4G)/1048576.00000000 W4G, SUM(W3G)/1048576.00000000 W3G,  
       SUM(W2G)/1048576.00000000 W2G, SUM(WOther)/1048576.00000000 WOther, 
       SUM(CASE WHEN UType = 'SMS' THEN cnt ELSE 0 END) SMS_cnt
FROM 
(
SELECT UDate, Subscriber_no, UType, COALESCE(minutes,0) minutes,cnt, 
            CAST(0 AS BIGINT) AS W4G, CAST(0 AS BIGINT)  AS W3G, 
            CAST(0 AS BIGINT)  AS W2G, CAST(0 AS BIGINT)  AS WOther   
  FROM cdrdm.V_TIT_sum_SMSVoice_CTN
  
  UNION ALL
  
  SELECT UDate, Subscriber_no, UType, 0 AS minutes,cnt, 
            W4G, W3G,W2G, WOther 
  FROM cdrdm.V_TIT_sum_UpDownBytes_CTN
) RP26CTN 
WHERE UDate BETWEEN '2022-05-01' AND '2022-05-31'
GROUP BY Subscriber_no;
