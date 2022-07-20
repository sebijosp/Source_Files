create or replace view stb_viewership.vw_viewership_sum as Select
CAN,
COMPANY_NUMBER,
ACCOUNT_NUMBER,
CHANNEL,
DEVICE_ID,
DURATION_SEC,
IS_SDV,
ZIP_CODE,
UPDATED_TS,
YEAR,
MONTH,
DAY
FROM stb_viewership.viewership_sum

