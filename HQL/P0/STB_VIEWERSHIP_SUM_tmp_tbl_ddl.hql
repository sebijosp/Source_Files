create table ext_stb_viewership.viewership_sum_tmp (
CAN	VARCHAR(20),
COMPANY_NUMBER	VARCHAR(5),
ACCOUNT_NUMBER	VARCHAR(15),
CHANNEL	INT,
DEVICE_ID	VARCHAR(40),
DURATION_SEC	INT,
IS_SDV	INT,
ZIP_CODE	VARCHAR(11),
UPDATED_TS TIMESTAMP,
YEAR	INT,
MONTH INT,
DAY INT)
STORED AS ORC

