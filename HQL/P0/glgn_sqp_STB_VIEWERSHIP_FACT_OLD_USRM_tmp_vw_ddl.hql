create or replace view ext_stb_viewership.viewership_fact_old_usrm_sqp_imp as
select

CAST(if(LOWER(CAN) = 'null', null, CAN) AS VARCHAR(20)) AS CAN,
CAST(null AS varchar(5)) AS COMPANY_NUMBER,
CAST(null AS VARCHAR(15)) AS ACCOUNT_NUMBER,
CAST(if(LOWER(CHANNEL) = 'null', null, CHANNEL) AS INT) AS CHANNEL,
CAST(if(LOWER(CHANNEL_DESC) = 'null', null, CHANNEL_DESC) AS VARCHAR(50)) AS CHANNEL_DESC,
CAST(if(LOWER(DEVICE_ID) = 'null', null, DEVICE_ID) AS VARCHAR(40)) AS DEVICE_ID,
CAST(if(LOWER(START_TIME) = 'null', null, from_unixtime(unix_timestamp(start_time, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP) AS START_VIEW,
CAST(if(LOWER(END_TIME) = 'null', null, from_unixtime(unix_timestamp(end_time, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP) AS END_VIEW,
CAST(if(LOWER(DURATION_SEC) = 'null', 0, DURATION_SEC) AS INT) AS DURATION_SEC,
CAST((CAST(if(LOWER(DURATION_SEC) = 'null', 0, DURATION_SEC) AS INT)/60) AS INT) AS DURATION_MIN,
CAST(if(LOWER(HUB_ID) = 'null', null, HUB_ID) AS INT) AS HUB_ID,
CAST(if(LOWER(IPG_CHANNEL) = 'null', null, IPG_CHANNEL) AS VARCHAR(50)) AS IPG_CHANNEL,
CAST(if(LOWER(IS_SDV) = null, null, IS_SDV) AS INT) AS IS_SDV,
CAST(if(LOWER(n_hour_end_time) = 'null', null, from_unixtime(unix_timestamp(n_hour_end_time, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP) AS N_HOUR_END_TIME,
CAST(if(LOWER(NODE_ID) = 'null', null, NODE_ID) AS VARCHAR(20)) AS NODE_ID,
CAST(if(LOWER(PRIVACY_OPT) = 'null', null, PRIVACY_OPT) AS INT) AS PRIVACY_OPT,
CAST(if(LOWER(QPSK_DEMOD_ID) = 'null', null, QPSK_DEMOD_ID) AS INT) AS QPSK_DEMOD_ID,
CAST(if(LOWER(QPSK_MOD_ID) = 'null', null, QPSK_MOD_ID) AS INT) AS QPSK_MOD_ID,
CAST(if(LOWER(SAM_CHANNEL) = 'null', null, SAM_CHANNEL) AS VARCHAR(50)) AS SAM_CHANNEL,
CAST(if(LOWER(SGID) = 'null', null, SGID) AS INT) AS SGID,
CAST(if(LOWER(SOURCE_ID) = 'null', null, SOURCE_ID) AS INT) AS SOURCE_ID,
CAST(if(LOWER(ZIP_CODE) = 'null', null, ZIP_CODE) AS VARCHAR(11)) AS ZIP_CODE,


CAST('OLD_USRM' AS VARCHAR(10)) as SOURCE_TYPE,
CAST(split(INPUT__FILE__NAME, '/userapps/glagoon/stb_viewership/old_usrm/landing/')[1] as varchar(100)) as SOURCE_FILENAME,
year(CAST(if(LOWER(START_TIME) = 'null', null, from_unixtime(unix_timestamp(start_time, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP)) AS YEAR,
month(CAST(if(LOWER(START_TIME) = 'null', null, from_unixtime(unix_timestamp(start_time, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP)) AS MONTH,
day(CAST(if(LOWER(START_TIME) = 'null', null, from_unixtime(unix_timestamp(start_time, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP)) AS DAY

FROM ext_stb_viewership.viewership_fact_old_usrm_src_file
where CAST(CONCAT(SUBSTRING(START_TIME,7,4), '-', SUBSTRING(START_TIME,1,2), '-', SUBSTRING(START_TIME,4,2)) AS DATE) >= date_sub(current_date, 10) and CAST(if(LOWER(START_TIME) = 'null', null, from_unixtime(unix_timestamp(start_time, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP) is not null
