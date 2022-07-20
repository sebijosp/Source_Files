DROP TABLE IF EXISTS MLMODEL_NONPI.NONPI_CCTS_BAN_SMB;

CREATE TABLE IF NOT EXISTS MLMODEL_NONPI.NONPI_CCTS_BAN_SMB
  (
    `BAN` VARCHAR(100) COMMENT '',
    `COMPLAIN_DATE` DATE COMMENT ''
  )
PARTITIONED BY (
    `RUNDT` DATE COMMENT ''
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

INSERT OVERWRITE TABLE MLMODEL_NONPI.NONPI_CCTS_BAN_SMB PARTITION(RUNDT)
SELECT
    tokenize(trim(BAN), 'K1'),
    COMPLAIN_DATE,
    RUNDT
FROM CERTUS.CCTS_BAN_SMB
WHERE RUNDT >=DATE_SUB(to_date('${hiveconf:DeltaPartStart}'), 14);
