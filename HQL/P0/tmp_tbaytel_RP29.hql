set hive.cli.print.header=true;
SET hive.execution.engine=tez;

SELECT PLMN, SUM(VOICE_Min) VOICE_Min, SUM(SMS_Cnt) SMS_Cnt, SUM(GPRS_MBytes) GPRS_MBytes
FROM cdrdm.V_Tbay_OB_DT_TAP_Agg_R29
WHERE UDate BETWEEN '2022-05-01' AND '2022-05-31'
GROUP BY PLMN, UDate;

