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

DROP VIEW IF EXISTS v_les_hist_all;

CREATE VIEW IF NOT EXISTS v_les_hist_all AS
SELECT
    z.subject_number,
    z.calling_serial_number,
    z.called_serial_number,
    z.imsi,
    z.call_timestamp,
    z.chargeable_duration,
    z.calling_number,
    z.called_number,
    z.original_called_number,
    z.redirecting_number,
    CASE WHEN z.file_name IN ('FACT_GSM_IN_VOICE_HIST', 'FACT_GSM_OUT_VOICE_HIST', 'FACT_GSM_CF_HIST', 'FACT_GSM_CFR_HIST', 'FACT_GSM_OUT_SMS_HIST', 'FACT_GSM_IN_SMS_HIST') THEN b.network_element
    ELSE z.switch_name END AS switch_id,
    IF (z.first_cell_id IS NOT NULL OR z.first_cell_id <> 0,
    (CASE WHEN z.file_name IN ('FACT_GSM_IN_VOICE_HIST', 'FACT_GSM_OUT_VOICE_HIST', 'FACT_GSM_CF_HIST', 'FACT_GSM_CFR_HIST', 'FACT_GSM_OUT_SMS_HIST', 'FACT_GSM_IN_SMS_HIST') THEN c.natural_key
    ELSE z.first_cell_id END), NULL) AS first_cell,
    IF (z.last_cell_id IS NOT NULL OR z.last_cell_id <> 0,
    (CASE WHEN z.file_name IN ('FACT_GSM_IN_VOICE_HIST', 'FACT_GSM_OUT_VOICE_HIST', 'FACT_GSM_CF_HIST', 'FACT_GSM_CFR_HIST', 'FACT_GSM_OUT_SMS_HIST', 'FACT_GSM_IN_SMS_HIST') THEN d.natural_key
    ELSE z.last_cell_id END), NULL) AS last_cell,
    z.record_seq_number,
    z.source_type,
    z.source_table,
    CASE WHEN z.file_name IN ('FACT_GSM_IN_VOICE_HIST', 'FACT_GSM_OUT_VOICE_HIST', 'FACT_GSM_CF_HIST', 'FACT_GSM_CFR_HIST', 'FACT_GSM_OUT_SMS_HIST', 'FACT_GSM_IN_SMS_HIST') THEN c.first_cell_id_extension
    ELSE z.first_cell_id_extension END AS first_cell_id_extension,
    z.call_timestamp_date
FROM
(
SELECT
    subscriber_no AS subject_number,
    
    CASE WHEN file_name IN ('FACT_GSM_OUT_VOICE_HIST', 'FACT_GSM_OUT_SMS_HIST') THEN
        imei
    ELSE
        CASE WHEN call_type IN (2,6) THEN imei ELSE NULL END
    END AS calling_serial_number,

    CASE WHEN file_name IN ('FACT_GSM_IN_VOICE_HIST', 'FACT_GSM_IN_SMS_HIST') THEN
        imei
    ELSE
        CASE WHEN call_type IN (5,8) THEN imei ELSE NULL END
    END AS called_serial_number,

    imsi,
    call_timestamp,
    chargeable_duration,

    CASE WHEN file_name IN ('FACT_GSM_OUT_VOICE_HIST', 'FACT_GSM_OUT_SMS_HIST') THEN subscriber_no
    WHEN file_name IN ('FACT_GSM_IN_VOICE_HIST', 'FACT_GSM_CF_HIST', 'FACT_GSM_CFR_HIST') THEN cast(calling_party_number as bigint)
    ELSE cleansed_calling_number END AS calling_number,

    CASE WHEN file_name IN ('FACT_GSM_IN_VOICE_HIST', 'FACT_GSM_IN_SMS_HIST') THEN subscriber_no
    WHEN file_name IN ('FACT_GSM_OUT_VOICE_HIST', 'FACT_GSM_CFR_HIST') THEN cast(called_party_number as bigint)
    WHEN file_name IN ('FACT_GSM_CF_HIST') THEN cast(redirecting_number as bigint)
    ELSE (CASE WHEN call_type = 4 THEN cleansed_redirecting_number ELSE cleansed_called_number END) END AS called_number,

    CASE WHEN file_name IN ('FACT_GSM_IN_VOICE_HIST', 'FACT_GSM_OUT_VOICE_HIST', 'FACT_GSM_CF_HIST', 'FACT_GSM_CFR_HIST') THEN cast(original_called_number as bigint)
    ELSE cleansed_original_number END AS original_called_number,

    CASE WHEN file_name IN ('FACT_GSM_IN_VOICE_HIST', 'FACT_GSM_OUT_VOICE_HIST', 'FACT_GSM_CFR_HIST') THEN cast(redirecting_number as bigint) 
    WHEN file_name IN ('FACT_GSM_CF_HIST') THEN cast(called_party_number as bigint)
    ELSE (CASE WHEN call_type = 4 THEN cleansed_called_number ELSE cleansed_redirecting_number END) END AS redirecting_number,
    NULL AS record_seq_number, -- always NULL
    1 AS source_type,
    'FACT_GSM_CDR' AS source_table,
    switch_name,
    file_name,
    first_cell_id,
    last_cell_id,
    first_cell_id_extension,
    call_timestamp_date AS call_timestamp_date
FROM cdrdm.FACT_GSM_CDR

UNION ALL

SELECT
    subscriber_no AS subject_number,
    NULL AS calling_serial_number,
    NULL AS called_serial_number,
    imsi AS imsi,
    CASE WHEN file_name IN ('FACT_SMS_HISTORY') THEN date_1 ELSE local_subscriber_timestamp END AS call_timestamp,
    NULL AS chargeable_duration,
    CASE WHEN charged_party = 'O' THEN subscriber_no ELSE other_msisdn END AS calling_number,
    CASE WHEN charged_party = 'T' THEN subscriber_no ELSE other_msisdn END AS called_number,
    NULL AS original_called_number,
    NULL AS redirecting_number,
    NULL AS record_seq_number, -- always NULL
    2 AS source_type,
    'FACT_SMS_CDR' AS source_table,
    switch_name,
    file_name,
    CAST('0' AS VARCHAR(15)) as first_cell_id,
    CAST('0' AS VARCHAR(15)) as last_cell_id,
    CAST(NULL AS VARCHAR(4)) AS first_cell_id_extension,
    local_subscriber_date AS call_timestamp_date
FROM cdrdm.FACT_SMS_CDR
) z 
LEFT OUTER JOIN cdrdm.dim_network_element b
ON switch_name = b.network_element_id
LEFT OUTER JOIN cdrdm.v_measured_object c
ON first_cell_id = c.measured_object_id
LEFT OUTER JOIN cdrdm.v_measured_object d
ON last_cell_id = d.measured_object_id;