SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
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
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.optimize.ppd=true;
set hive.mapjoin.smalltable.filesize=40000000;

DROP TABLE IF EXISTS CDRDM.STM_USAGE_STEP3;
CREATE TABLE CDRDM.STM_USAGE_STEP3 AS
SELECT
  UPPER(B.PLMN_NAME) AS NW_CARRIER,
  D_GSM1215.record_opening_date AS DT,
  D_GSM1215.SERVE_SID AS SID,
  CAST(D_GSM1215.SERVED_IMSI AS BIGINT) AS IMSI,
  SUM(0) AS TOT_SMS,
  SUM(0) AS TOT_VOICE_MIN,
  CAST(SUM(NVL(D_GSM1215.DATA_VOLUME_UPLINK_ARCHIVE,0)) AS DECIMAL(32,0))+
    CAST(SUM(NVL(D_GSM1215.DATA_VOLUME_DOWNLINK_ARCHIVE,0)) AS DECIMAL(32,0) )AS  DATA_BYTES
  FROM (
SELECT A.RECORD_OPENING_DATE, A.DATA_VOLUME_UPLINK_ARCHIVE, A.DATA_VOLUME_DOWNLINK_ARCHIVE,A.GPRS_CHOICE_MASK_ARCHIVE, A.SERVED_IMSI, A.CELL_ID, Rt.ROUTE_ID, Rt.SERVE_SID
FROM cdrdm.v_chatr_occ_gprs_cdr A JOIN cdrdm.stg_route Rt
ON  CONCAT(SUBSTR('00000000',1,8-LENGTH(A.Cell_Id)), TRIM(A.Cell_ID)) = rt.route_id_hex
WHERE A.GPRS_CHOICE_MASK_ARCHIVE = '0001'
AND SUBSTR(A.SERVED_IMSI,1,6) NOT IN ('302720', '302370','302820')
AND A.RECORD_OPENING_DATE between '${hiveconf:date_from}' AND '${hiveconf:date_to}'
) D_GSM1215
JOIN ela_v21.plmn_settlement_gg B ON SUBSTR(D_GSM1215.SERVED_IMSI,1,6) = B.PLMN_CODE
GROUP BY UPPER(B.PLMN_NAME),D_GSM1215.record_opening_date,D_GSM1215.SERVE_SID,(CAST(D_GSM1215.SERVED_IMSI AS BIGINT))
;

