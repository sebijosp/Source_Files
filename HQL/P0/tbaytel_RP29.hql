set hive.cli.print.header=true;
SET hive.execution.engine=tez;

SELECT PLMN, SUM(VOICE_Min) VOICE_Min, SUM(SMS_Cnt) SMS_Cnt, SUM(GPRS_MBytes) GPRS_MBytes
FROM cdrdm.V_Tbay_OB_DT_TAP_Agg_R29
WHERE {var_date_filter}
GROUP BY PLMN, UDate;

