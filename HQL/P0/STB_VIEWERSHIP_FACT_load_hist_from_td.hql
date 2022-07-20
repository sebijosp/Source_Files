set hive.exec.max.created.files=2000000 ;
insert overwrite table stb_viewership.viewership_fact partition(year, month, day) 
select
ACCT_NUM as CAN,
cast(null as varchar(5)) AS COMPANY_NUMBER,
cast(null as varchar(15)) AS ACCOUNT_NUMBER,
CHANNEL,
CHANNEL_DESC,
DEVICE_ID,
START_VIEW,
END_VIEW,
DURATION_SEC,
DURATION_MIN,
HUB_ID,
IPG_CHANNEL,
IS_SDV,
N_HOUR_END_TIME,
NODE_ID,
PRIVACY_OPT,
QPSK_DEMOD_ID,
QPSK_MOD_ID,
SAM_CHANNEL,
SGID,
SOURCE_ID,
ZIP_CODE2,
'OLD_USRM' as SOURCE_TYPE,
'HIST_LOAD_FROM_TERADATA' as SOURCE_FILENAME,
CURRENT_TIMESTAMP() AS DATALAKE_LOAD_TS,
YEAR(START_VIEW),
MONTH(START_VIEW),
DAY(START_VIEW)

FROM ucar.hist_tdprod_tvviewershipvws_history_sdv 
where sqoop_ext_date <= '2016-12-31' and YEAR(START_VIEW) is not null
