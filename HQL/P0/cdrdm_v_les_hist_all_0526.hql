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

DROP VIEW IF EXISTS cdrdm.v_les_hist_all;

CREATE VIEW IF NOT EXISTS cdrdm.v_les_hist_all AS
SELECT
    subscriber_no AS subject_number,
    CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
    CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
    imsi AS imsi,
    call_timestamp AS call_timestamp,
    chargeable_duration AS chargeable_duration,
    cleansed_calling_number AS calling_number,
    CASE WHEN call_type = 4 THEN cleansed_redirecting_number ELSE cleansed_called_number END AS called_number,
    cleansed_original_number AS original_called_number,
    CASE WHEN call_type = 4 THEN cleansed_called_number ELSE cleansed_redirecting_number END AS redirecting_number,
    CASE WHEN exchange_identity = 'RGRANSIMSCCM07E' THEN 'TO2BC06' ELSE SUBSTR(exchange_identity, 1,(instr(exchange_identity,'_') -1)) END switch_id,
    --CAST(0 AS SMALLINT) AS switch_id_hist,
    first_cell_id AS first_cell,
    --CAST(0 AS INT) AS first_cell_hist,
    last_cell_id AS last_cell,
    --CAST(0 AS INT) AS last_cell_hist,
    NULL AS record_seq_number,
    1  AS source_type,
    'FACT_GSM_CDR' AS source_table,
    first_cell_id_extension,
    CAST(call_timestamp_date AS STRING)
FROM cdrdm.FACT_GSM_CDR

UNION ALL

SELECT
    subscriber_no AS subject_number,
    CAST(NULL AS VARCHAR(16)) AS calling_serial_number,
    CAST(NULL AS VARCHAR(16)) AS called_serial_number,
    imsi AS imsi,
    local_subscriber_timestamp AS call_timestamp,
    CAST(NULL AS INT) AS chargeable_duration,
    CASE WHEN charged_party = 'O' THEN subscriber_no ELSE other_msisdn END AS calling_number,
    CASE WHEN charged_party = 'T' THEN subscriber_no ELSE other_msisdn END AS called_number,
    CAST(NULL AS BIGINT) AS original_called_number,
    CAST(NULL AS BIGINT) AS redirecting_number,
    switch_name AS switch_id,
    --CAST(NULL AS VARCHAR(15)) AS switch_id_curr,
    --switch_id AS switch_id_hist,
    CAST('0' AS VARCHAR(15)) AS first_cell,
    --CAST(0 AS INTEGER) AS first_cell_hist,
    CAST('0' AS VARCHAR(15)) AS last_cell,
    --CAST(0 AS INTEGER) AS last_cell_hist,
    CAST(NULL AS INT) AS record_seq_number,
    2 AS source_type,
    'FACT_SMS_CDR' AS source_table,
    CAST(NULL AS VARCHAR(4)) AS first_cell_id_extension,
    CAST(local_subscriber_date AS STRING) AS call_timestamp_date
FROM cdrdm.FACT_SMS_CDR;