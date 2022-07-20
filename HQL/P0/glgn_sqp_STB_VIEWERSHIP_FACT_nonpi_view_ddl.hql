create or replace view stb_viewership_nonpi.vw_viewership_fact  as select
if(CAN IS NOT NULL,cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(CAN AS STRING))) AS VARCHAR(64)),NULL) AS CAN,
COMPANY_NUMBER,
if(ACCOUNT_NUMBER IS NOT NULL,cast(reflect ('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(ACCOUNT_NUMBER AS STRING))) AS VARCHAR(64)),NULL) AS ACCOUNT_NUMBER,
CHANNEL,
CHANNEL_DESC,
if(DEVICE_ID IS NOT NULL,cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(DEVICE_ID AS STRING))) AS VARCHAR(64)),NULL) AS DEVICE_ID,
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
if(ZIP_CODE IS NOT NULL, substr(ZIP_CODE, 1, 3), NULL) AS ZIP_CODE,
SOURCE_TYPE,
YEAR,
MONTH,
DAY,
SOURCE_FILENAME,
DATALAKE_LOAD_TS
FROM stb_viewership.viewership_fact
