create table stb_viewership.viewership_fact (
CAN	VARCHAR(20),
COMPANY_NUMBER	VARCHAR(5),
ACCOUNT_NUMBER	VARCHAR(15),
CHANNEL	INT,
CHANNEL_DESC	VARCHAR(50),
DEVICE_ID	VARCHAR(40),
START_VIEW	TIMESTAMP,
END_VIEW	TIMESTAMP,
DURATION_SEC	INT,
DURATION_MIN	INT,
HUB_ID	INT,
IPG_CHANNEL	VARCHAR(50),
IS_SDV	INT,
N_HOUR_END_TIME	TIMESTAMP,
NODE_ID	VARCHAR(20),
PRIVACY_OPT	INT,
QPSK_DEMOD_ID	INT,
QPSK_MOD_ID	INT,
SAM_CHANNEL	VARCHAR(50),
SGID	INT,
SOURCE_ID	INT,
ZIP_CODE	VARCHAR(11),
SOURCE_TYPE	VARCHAR(10),
SOURCE_FILENAME VARCHAR(100),
DATALAKE_LOAD_TS TIMESTAMP)

partitioned by (YEAR	INT, MONTH INT, DAY INT)
STORED AS ORC

