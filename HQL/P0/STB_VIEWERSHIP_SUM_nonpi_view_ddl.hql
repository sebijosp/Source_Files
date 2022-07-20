create or replace view stb_viewership_nonpi.vw_viewership_sum as Select
if(CAN IS NOT NULL,cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(CAN AS STRING))) AS VARCHAR(64)),NULL) AS CAN,
COMPANY_NUMBER,
if(ACCOUNT_NUMBER IS NOT NULL,cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(ACCOUNT_NUMBER AS STRING))) AS VARCHAR(64)),NULL) AS ACCOUNT_NUMBER,
CHANNEL,
if(DEVICE_ID IS NOT NULL,cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(DEVICE_ID AS STRING))) AS VARCHAR(64)),NULL) AS DEVICE_ID,
DURATION_SEC,
IS_SDV,
if(ZIP_CODE IS NOT NULL, substr(ZIP_CODE, 1, 3), NULL) AS ZIP_CODE,
UPDATED_TS,
YEAR,
MONTH,
DAY
FROM stb_viewership.viewership_sum

