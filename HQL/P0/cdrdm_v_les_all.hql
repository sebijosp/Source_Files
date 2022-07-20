SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;

USE cdrdm;

DROP VIEW IF EXISTS v_les_all;

CREATE VIEW IF NOT EXISTS v_les_all AS
SELECT
    subscriber_no AS subject_number,
    CASE WHEN call_type IN (2,6) THEN imei ELSE NULL END AS calling_serial_number,
    CASE WHEN call_type IN (5,8) THEN imei ELSE NULL END AS called_serial_number,
    imsi AS imsi,
    call_timestamp AS call_timestamp,
    chargeable_duration AS chargeable_duration,
    cleansed_calling_number AS calling_number,
    CASE WHEN call_type = 4 THEN cleansed_redirecting_number ELSE cleansed_called_number END AS called_number,
    cleansed_original_number AS original_called_number,
    CASE WHEN call_type = 4 THEN cleansed_called_number ELSE cleansed_redirecting_number END AS redirecting_number,
    switch_name AS switch_id,
    IF (first_cell_id IS NOT NULL OR first_cell_id <> 0, first_cell_id, NULL) AS first_cell,
    IF (last_cell_id IS NOT NULL OR last_cell_id <> 0, last_cell_id, NULL) AS last_cell,
    NULL AS record_seq_number, -- always NULL
    1 AS source_type,
    'FACT_GSM_CDR' AS source_table,
    first_cell_id_extension AS first_cell_id_extension,
    call_timestamp_date AS call_timestamp_date
FROM cdrdm.FACT_GSM_CDR

UNION ALL

SELECT
    subscriber_no AS subject_number,
    NULL AS calling_serial_number,
    NULL AS called_serial_number,
    imsi AS imsi,
    local_subscriber_timestamp AS call_timestamp,
    NULL AS chargeable_duration,
    CASE WHEN charged_party = 'O' THEN subscriber_no ELSE other_msisdn END AS calling_number,
    CASE WHEN charged_party = 'T' THEN subscriber_no ELSE other_msisdn END AS called_number,
    NULL AS original_called_number,
    NULL AS redirecting_number,
    switch_name AS switch_id,
    CAST('0' AS VARCHAR(15)) AS first_cell,
    CAST('0' AS VARCHAR(15)) AS last_cell,
    NULL AS record_seq_number, -- always NULL
    2 AS source_type,
    'FACT_SMS_CDR' AS source_table,
    CAST(NULL AS VARCHAR(4)) AS first_cell_id_extension,
    local_subscriber_date AS call_timestamp_date
FROM cdrdm.FACT_SMS_CDR;