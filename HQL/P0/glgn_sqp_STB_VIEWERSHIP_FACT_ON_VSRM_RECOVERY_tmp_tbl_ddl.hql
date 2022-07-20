create external table ext_stb_viewership.viewership_fact_on_vsrm_recovery_src_file (
CAN	VARCHAR(20),
CHANNEL	INT,
CHANNEL_DESC	 VARCHAR(5),
DEVICE_ID	VARCHAR(40),
DURATION_SEC	INT,
END_TIME STRING,
HUB_ID	INT,
IPG_CHANNEL	 VARCHAR(50),
IS_SDV	INT,
N_HOUR_END_TIME	STRING,
NODE_ID	 VARCHAR(20),
PRIVACY_OPT	INT,
QPSK_DEMOD_ID	INT,
QPSK_MOD_ID	INT,
SAM_CHANNEL	VARCHAR(50),
SGID	INT,
SOURCE_ID	INT,
START_TIME	STRING,
ZIP_CODE	VARCHAR(11))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = "\,",
"quoteChar"     = "\""
)
STORED AS TEXTFile
LOCATION '/userapps/glagoon/stb_viewership/ON/vsrm/landing_New' ;
