CREATE OR REPLACE VIEW `iptv`.`locker_purchase_fct_incr_vw` AS WITH REF_TBL AS (
SELECT
nonetl_iptv_job_relatn_ref.job_name           ,
nonetl_iptv_job_relatn_ref.job_type           ,
nonetl_iptv_job_relatn_ref.job_domain         ,
nonetl_iptv_job_relatn_ref.job_status         ,
nonetl_iptv_job_relatn_ref.job_execution_range,
nonetl_iptv_job_relatn_ref.iptv_pipeline      ,
nonetl_iptv_job_relatn_ref.iptv_job_name      ,
nonetl_iptv_job_relatn_ref.iptv_source_table  ,
GREATEST(nonetl_iptv_job_relatn_ref.etl_updt_dt,nonetl_iptv_job_relatn_ref.etl_insrt_dt) AS LAST_REF_CHANGE,
COUNT(nonetl_iptv_job_relatn_ref.job_name) OVER (PARTITION BY nonetl_iptv_job_relatn_ref.iptv_pipeline, nonetl_iptv_job_relatn_ref.job_execution_range) AS PIPELINE_CNT
FROM IPTV.NONETL_IPTV_JOB_RELATN_REF
WHERE nonetl_iptv_job_relatn_ref.job_status = 'ACTIVE'
),
REF_FLTR AS (
SELECT
ref_tbl.job_name           ,
ref_tbl.job_type           ,
ref_tbl.job_domain         ,
ref_tbl.job_status         ,
ref_tbl.job_execution_range,
ref_tbl.iptv_pipeline      ,
ref_tbl.iptv_job_name      ,
ref_tbl.iptv_source_table  ,
ref_tbl.pipeline_cnt
FROM REF_TBL
WHERE ref_tbl.job_type   = 'SCORECARD'
AND   ref_tbl.job_domain = 'HIVE'
AND   ref_tbl.job_execution_range = 'D1'
AND   ref_tbl.iptv_source_table   = 'IPTV.LOCKER_PURCHASE_FCT'
),
BDM_CONTROL AS (
SELECT
ctl.iptv_end_curr_max_value,
ctl.job_name           ,
ref.job_execution_range,
ref.iptv_pipeline      ,
ref.iptv_job_name      ,
ctl.last_run_date      ,
ref.pipeline_cnt        ,
TO_DATE(ctl.execution_start_ts) AS EXEC_DT
FROM REF_FLTR FLT
INNER JOIN REF_TBL REF
ON  flt.iptv_pipeline = ref.iptv_pipeline
AND flt.job_execution_range = ref.job_execution_range
INNER JOIN IPTV.NONETL_JOB_EXECUTION_CNTRL CTL
ON ref.job_name = ctl.job_name
AND ctl.execution_status = 'SUCCEEDED'
WHERE ctl.etl_insrt_dt >= ref.last_ref_change
),
BDM_CONTROL_CNT AS (
SELECT bdm_control.iptv_pipeline, bdm_control.job_execution_range, bdm_control.exec_dt, COUNT(DISTINCT bdm_control.job_name) AS TOTAL_SUCCESS_CNT
FROM BDM_CONTROL
GROUP BY bdm_control.iptv_pipeline, bdm_control.job_execution_range, bdm_control.exec_dt
),
COMPLETED_CYCLES AS (
SELECT
a.job_name     ,
a.iptv_job_name,
MAX(a.iptv_end_curr_max_value) AS MAXVAL_OF_LAST_FIN_CYCLE,
MAX(a.last_run_date)           AS RUNDT_OF_LAST_FIN_CYCLE
FROM BDM_CONTROL_CNT B
INNER JOIN BDM_CONTROL A
ON a.iptv_pipeline = b.iptv_pipeline
AND a.job_execution_range = b.job_execution_range
AND a.exec_dt = b.exec_dt
WHERE b.total_success_cnt >= a.pipeline_cnt
AND a.job_name in (SELECT ref_fltr.job_name FROM REF_FLTR)
GROUP BY a.job_name,a.iptv_job_name
),
GLGN_CTL AS (
SELECT
rcf.job_name AS GLGN_JOB_NAME,
CAST(rcf.delta_col_prev_max_value AS TIMESTAMP) AS ETL_MIN_VALUE,
CAST(rcf.delta_col_curr_max_value AS TIMESTAMP) AS ETL_MAX_VALUE
FROM REF_FLTR FLT
INNER JOIN CFW_HDP.GLGN_JOB_EXECUTION RCF
ON rcf.job_name = flt.iptv_job_name
WHERE rcf.execution_status = 'SUCCEEDED'
),
CFW AS (
SELECT
etl.glgn_job_name,
MIN(TO_DATE(etl.etl_min_value)) AS MIN_VALUE,
MAX(TO_DATE(etl.etl_max_value)) AS MAX_VALUE,
MIN(etl.etl_min_value) AS MIN_VALUE_TS,
MAX(etl.etl_max_value) AS MAX_VALUE_TS,
1 as dummy
FROM GLGN_CTL ETL
LEFT OUTER JOIN COMPLETED_CYCLES CTL
ON etl.glgn_job_name = ctl.iptv_job_name
WHERE etl.etl_min_value >= ctl.maxval_of_last_fin_cycle
OR    etl.etl_max_value >= CURRENT_DATE()
GROUP BY etl.glgn_job_name
)
SELECT 
`compact`.`uuid`,
`compact`.`timestamp`,
`compact`.`hostname`,
`compact`.`traceid`,
`compact`.`spanid`,
`compact`.`parentspanid`,
`compact`.`spanname`,
`compact`.`appname`,
`compact`.`money_starttime`,
`compact`.`spanduration`,
`compact`.`spansuccess`,
`compact`.`partner`,
`compact`.`receiverid`,
`compact`.`deviceid`,
`compact`.`devicesourceid`,
`compact`.`account`,
`compact`.`accountsourceid`,
`compact`.`billingaccountid`,
`compact`.`macaddress`,
`compact`.`ecmmacaddress`,
`compact`.`firmwareversion`,
`compact`.`devicetype`,
`compact`.`make`,
`compact`.`model`,
`compact`.`partner` `device_partner`,
`compact`.`ipaddress`,
`compact`.`utcoffset`,
`compact`.`postalcode`,
`compact`.`dma`,
`compact`.`action`,
`compact`.`transactionid`,
`compact`.`partnertransactionid`,
`compact`.`transactiontime`,
`compact`.`description`,
`compact`.`offerid`,
`compact`.`pricetopurchaser`,
`compact`.`currencycode`,
`compact`.`preorder`,
`compact`.`revokereason`,
`compact`.`revoketimestamp`,
`compact`.`assetid`,
`compact`.`title`,
`compact`.`assetright_starttime`,
`compact`.`endtime`,
`compact`.`purchasecategory`,
`compact`.`hdp_file_name`,
`compact`.`hdp_create_ts`,
`compact`.`hdp_update_ts`,
`compact`.`event_date`
FROM (
SELECT
`f`.`header`.`uuid`,
`f`.`header`.`timestamp`,
`f`.`header`.`hostname`,
`f`.`header`.`money`.`traceid`,
`f`.`header`.`money`.`spanid`,
`f`.`header`.`money`.`parentspanid`,
`f`.`header`.`money`.`spanname`,
`f`.`header`.`money`.`appname`,
`f`.`header`.`money`.`starttime` `money_starttime`,
`f`.`header`.`money`.`spanduration`,
`f`.`header`.`money`.`spansuccess`,
`f`.`partner`,
`f`.`device`.`receiverid`,
`f`.`device`.`deviceid`,
`f`.`device`.`devicesourceid`,
`f`.`device`.`account`,
`f`.`device`.`accountsourceid`,
`f`.`device`.`billingaccountid`,
`f`.`device`.`macaddress`,
`f`.`device`.`ecmmacaddress`,
`f`.`device`.`firmwareversion`,
`f`.`device`.`devicetype`,
`f`.`device`.`make`,
`f`.`device`.`model`,
`f`.`device`.`partner` `device_partner`,
`f`.`device`.`ipaddress`,
`f`.`device`.`utcoffset`,
`f`.`device`.`postalcode`,
`f`.`device`.`dma`,
`f`.`action`,
`f`.`transactionid`,
`f`.`partnertransactionid`,
`f`.`transactiontime`,
`f`.`description`,
`f`.`offerid`,
`f`.`pricetopurchaser`,
`f`.`currencycode`,
`f`.`preorder`,
`f`.`revokereason`,
`f`.`revoketimestamp`,
`f`.`assetright`.`assetid`,
`f`.`assetright`.`title`,
`f`.`assetright`.`starttime` `assetright_starttime`,
`f`.`assetright`.`endtime`,
`f`.`assetright`.`purchasecategory`,
`f`.`hdp_file_name`,
`f`.`hdp_create_ts`,
`f`.`hdp_update_ts`,
`f`.`event_date`,
1 as dummy
FROM `iptv`.`locker_purchase_fct` `F`
where `f`.`event_date` between  date_sub(current_date, 7) and date_sub(current_date, 1)
) COMPACT
JOIN `cfw` on `compact`.`dummy` =`cfw`.`dummy`
where `compact`.`hdp_create_ts` between `cfw`.`min_value_ts`
and `cfw`.`max_value_ts`;
