DROP TABLE IF EXISTS MLMODEL_NONPI.NONPI_VIEWERSHIP_FACT;

CREATE TABLE IF NOT EXISTS MLMODEL_NONPI.NONPI_VIEWERSHIP_FACT
  (
    `CAN` VARCHAR(100) COMMENT '',
    `COMPANY_NUMBER` VARCHAR(5) COMMENT '',
    `ACCOUNT_NUMBER` VARCHAR(100) COMMENT '',
    `CHANNEL` INT COMMENT '',
    `CHANNEL_DESC` VARCHAR(50) COMMENT '',
    `DEVICE_ID` VARCHAR(100) COMMENT '',
    `START_VIEW` TIMESTAMP COMMENT '',
    `END_VIEW` TIMESTAMP COMMENT '',
    `DURATION_SEC` INT COMMENT '',
    `DURATION_MIN` INT COMMENT '',
    `HUB_ID` INT COMMENT '',
    `IPG_CHANNEL` VARCHAR(50) COMMENT '',
    `IS_SDV` INT COMMENT '',
    `N_HOUR_END_TIME` TIMESTAMP COMMENT '',
    `NODE_ID` VARCHAR(20) COMMENT '',
    `PRIVACY_OPT` INT COMMENT '',
    `QPSK_DEMOD_ID` INT COMMENT '',
    `QPSK_MOD_ID` INT COMMENT '',
    `SAM_CHANNEL` VARCHAR(50) COMMENT '',
    `SGID` INT COMMENT '',
    `SOURCE_ID` INT COMMENT '',
    `ZIP_CODE` VARCHAR(11) COMMENT '',
    `SOURCE_TYPE` VARCHAR(10) COMMENT '',
    `SOURCE_FILENAME` VARCHAR(100) COMMENT '',
    `DATALAKE_LOAD_TS` TIMESTAMP COMMENT ''
  )
PARTITIONED BY (
    `YEAR` INT COMMENT '',
    `MONTH` INT COMMENT '',
    `DAY` INT COMMENT ''
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

INSERT OVERWRITE TABLE MLMODEL_NONPI.NONPI_VIEWERSHIP_FACT PARTITION(YEAR, MONTH, DAY)
SELECT
    tokenize(trim(CAN), 'K1'),
    COMPANY_NUMBER,
    tokenize(trim(ACCOUNT_NUMBER), 'K1'),
    CHANNEL,
    CHANNEL_DESC,
    tokenize(trim(DEVICE_ID), 'K10'),
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
    ZIP_CODE,
    SOURCE_TYPE,
    SOURCE_FILENAME,
    DATALAKE_LOAD_TS,
    YEAR,
    MONTH,
    DAY
FROM STB_VIEWERSHIP.VIEWERSHIP_FACT;
