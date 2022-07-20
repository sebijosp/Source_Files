set hive.cli.print.header=true;
SET hive.execution.engine=tez;

SELECT RID, Route_Desc,
          SUM(LD_US_VMIN) LD_US_VMIN,  
          SUM(LD_CANADA_VMIN) LD_CANADA_VMIN, 
          SUM(LD_INTL_VMIN) LD_INTL_VMIN, 
          SUM(LD_INTL_SMS_Cnt) LD_INTL_SMS_Cnt
FROM cdrdm.V_Tbay_DT_LD_CNLD_Agg_R28
WHERE UDate BETWEEN '2022-05-01' AND '2022-05-31'
GROUP BY RID, Route_Desc;
