CREATE OR REPLACE VIEW cdrdm.vlesrhpinfo AS 
SELECT
fact_rhpc_dpscsv_vq_daily.subscriber_no AS subscriber_no,
CASE
WHEN fact_rhpc_dpscsv_vq_daily.call_type = '110' THEN 'Long Distance'
WHEN (fact_rhpc_dpscsv_vq_daily.call_type IN ('126','127','128')) THEN 'Local'
WHEN cast(fact_rhpc_dpscsv_vq_daily.call_type as  int) = 6 THEN 'Call free'
ELSE fact_rhpc_dpscsv_vq_daily.call_type
END AS call_type,
dim_rhpc_call_type.description AS Call_Type_description,
fact_rhpc_dpscsv_vq_daily.field_date AS field_date,
fact_rhpc_dpscsv_vq_daily.field_date AS CALL_DATE,
fact_rhpc_dpscsv_vq_daily.answer_time AS answer_time,
SUM(CASE WHEN (fact_rhpc_dpscsv_vq_daily.elapsed_time > 0) THEN ((fact_rhpc_dpscsv_vq_daily.elapsed_time / 1000) * 60) ELSE 0 END) AS elapsed_time,
CASE WHEN (fact_rhpc_dpscsv_vq_daily.answer_ind = 0) THEN 'Answered' ELSE 'Not Answered' END AS answer_ind,
fact_rhpc_dpscsv_vq_daily.term_number AS term_number,
fact_rhpc_dpscsv_vq_daily.call_type AS call_type_code,
fact_rhpc_dpscsv_vq_daily.class_feature_code AS class_feature_code,
fact_rhpc_dpscsv_vq_daily.farend_number AS farend_number,
fact_rhpc_dpscsv_vq_daily.field_time AS field_time,
fact_rhpc_dpscsv_vq_daily.switch_name AS Switch_ID
FROM cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY
LEFT OUTER JOIN cdrdm.dim_rhpc_call_type
ON fact_rhpc_dpscsv_vq_daily.call_type = CAST(lpad(dim_rhpc_call_type.call_type_cd,3,'0') AS CHAR(3))
GROUP BY fact_rhpc_dpscsv_vq_daily.subscriber_no, fact_rhpc_dpscsv_vq_daily.call_type, dim_rhpc_call_type.description, fact_rhpc_dpscsv_vq_daily.field_date, fact_rhpc_dpscsv_vq_daily.answer_time, fact_rhpc_dpscsv_vq_daily.answer_ind, fact_rhpc_dpscsv_vq_daily.term_number, fact_rhpc_dpscsv_vq_daily.class_feature_code, fact_rhpc_dpscsv_vq_daily.farend_number, fact_rhpc_dpscsv_vq_daily.field_time, fact_rhpc_dpscsv_vq_daily.switch_name

UNION ALL

SELECT
FACT_RHP_MAESTRO.subscriber_no AS subscriber_no,
CASE
WHEN FACT_RHP_MAESTRO.call_type = '110' THEN 'Long Distance'
WHEN (FACT_RHP_MAESTRO.call_type IN ('126','127','128')) THEN 'Local'
WHEN cast(FACT_RHP_MAESTRO.call_type as int) = 6 THEN 'Call free'
ELSE FACT_RHP_MAESTRO.call_type
END AS call_type,
dim_rhpc_call_type.description AS Call_Type_description,
FACT_RHP_MAESTRO.field_date AS field_date,
FACT_RHP_MAESTRO.field_date AS CALL_DATE,
FACT_RHP_MAESTRO.answer_time AS answer_time,
SUM(CASE WHEN (FACT_RHP_MAESTRO.elapsed_time > 0) THEN ((FACT_RHP_MAESTRO.elapsed_time / 1000) * 60) ELSE 0 END) AS elapsed_time,
CASE WHEN (FACT_RHP_MAESTRO.answer_ind = 0) THEN 'Answered' ELSE 'Not Answered' END AS answer_ind,
FACT_RHP_MAESTRO.term_number AS term_number,
FACT_RHP_MAESTRO.call_type AS call_type_code,
FACT_RHP_MAESTRO.class_feature_code AS class_feature_code,
FACT_RHP_MAESTRO.farend_number AS farend_number,
FACT_RHP_MAESTRO.field_time AS field_time,
FACT_RHP_MAESTRO.switch_name AS Switch_ID
FROM cdrdm.FACT_RHP_MAESTRO
LEFT OUTER JOIN cdrdm.dim_rhpc_call_type
ON FACT_RHP_MAESTRO.call_type = CAST(lpad(dim_rhpc_call_type.call_type_cd,3,'0') AS CHAR(3))
GROUP BY FACT_RHP_MAESTRO.subscriber_no, FACT_RHP_MAESTRO.call_type, dim_rhpc_call_type.description, FACT_RHP_MAESTRO.field_date, FACT_RHP_MAESTRO.answer_time, FACT_RHP_MAESTRO.answer_ind, FACT_RHP_MAESTRO.term_number, FACT_RHP_MAESTRO.class_feature_code, FACT_RHP_MAESTRO.farend_number, FACT_RHP_MAESTRO.field_time, FACT_RHP_MAESTRO.switch_name

UNION ALL

SELECT
FACT_RHP_MANUAL.subscriber_no AS subscriber_no,
CASE
WHEN FACT_RHP_MANUAL.call_type = '110' THEN 'Long Distance'
WHEN (FACT_RHP_MANUAL.call_type IN ('126','127','128')) THEN 'Local'
WHEN cast(FACT_RHP_MANUAL.call_type as int) = 6  THEN 'Call free'
ELSE FACT_RHP_MANUAL.call_type
END AS call_type,
dim_rhpc_call_type.description AS Call_Type_description,
FACT_RHP_MANUAL.field_date AS field_date,
FACT_RHP_MANUAL.field_date AS CALL_DATE,
FACT_RHP_MANUAL.answer_time AS answer_time,
SUM(CASE WHEN (FACT_RHP_MANUAL.elapsed_time > 0) THEN ((FACT_RHP_MANUAL.elapsed_time / 1000) * 60) ELSE 0 END) AS elapsed_time,
CASE WHEN (FACT_RHP_MANUAL.answer_ind = 0) THEN 'Answered' ELSE 'Not Answered' END AS answer_ind,
FACT_RHP_MANUAL.term_number AS term_number,
FACT_RHP_MANUAL.call_type AS call_type_code,
FACT_RHP_MANUAL.class_feature_code AS class_feature_code,
FACT_RHP_MANUAL.farend_number AS farend_number,
FACT_RHP_MANUAL.field_time AS field_time,
FACT_RHP_MANUAL.switch_name AS Switch_ID
FROM cdrdm.FACT_RHP_MANUAL
LEFT OUTER JOIN cdrdm.dim_rhpc_call_type
ON FACT_RHP_MANUAL.call_type = CAST(lpad(dim_rhpc_call_type.call_type_cd,3,'0') AS CHAR(3))
GROUP BY FACT_RHP_MANUAL.subscriber_no, FACT_RHP_MANUAL.call_type, dim_rhpc_call_type.description, FACT_RHP_MANUAL.field_date, FACT_RHP_MANUAL.answer_time, FACT_RHP_MANUAL.answer_ind, FACT_RHP_MANUAL.term_number, FACT_RHP_MANUAL.class_feature_code, FACT_RHP_MANUAL.farend_number, FACT_RHP_MANUAL.field_time, FACT_RHP_MANUAL.switch_name
;


