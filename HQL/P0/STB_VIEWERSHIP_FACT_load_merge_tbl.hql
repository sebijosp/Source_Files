truncate table ext_stb_viewership.viewership_fact_sqp_imp_merge;

set hive.execution.engine=mr;
set hive.optimize.sort.dynamic.partition=false;
set set mapreduce.task.timeout=3600000;

insert into ext_stb_viewership.viewership_fact_sqp_imp_merge
select
if(acct.COMPANY_NUMBER is null OR acct.ACCOUNT_NUMBER is null, vw.CAN, concat(acct.COMPANY_NUMBER, acct.ACCOUNT_NUMBER)),
if(acct.COMPANY_NUMBER is null, if(vw.CAN is null, null, substr(vw.CAN,1,3)), acct.COMPANY_NUMBER),
if(acct.ACCOUNT_NUMBER is null, if(vw.CAN is null, null, substr(vw.CAN,4)), acct.ACCOUNT_NUMBER),
vw.CHANNEL,
vw.CHANNEL_DESC,
vw.DEVICE_ID,
vw.START_VIEW,
vw.END_VIEW,
vw.DURATION_SEC,
vw.DURATION_MIN,
vw.HUB_ID,
vw.IPG_CHANNEL,
vw.IS_SDV,
vw.N_HOUR_END_TIME,
vw.NODE_ID,
vw.PRIVACY_OPT,
vw.QPSK_DEMOD_ID,
vw.QPSK_MOD_ID,
vw.SAM_CHANNEL,
vw.SGID,
vw.SOURCE_ID,
acct.POSTAL_ZIP_CODE,
acct.COMPANY_NAME,
vw.SOURCE_FILENAME,
vw.DATALAKE_LOAD_TS,
year,
month,
day
FROM stb_viewership.viewership_fact vw left outer join ext_stb_viewership.acct_zipcode_tmp acct on substr(vw.device_id, 1, 17) = acct.DEVICE_ID
where concat(vw.year, "-", lpad(vw.month, 2, '0'), "-", lpad(vw.day, 2, '0')) >= date_sub(current_date, 60);