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

DROP TABLE IF EXISTS CDRDM.STM_USAGE_STEP4;
CREATE TABLE IF NOT EXISTS CDRDM.STM_USAGE_STEP4 AS
 SELECT
    CASE
     WHEN SP.SP_NAME IS NOT NULL OR SP.SP_NAME <> '' THEN UPPER(SP.SP_NAME)
     ELSE 'CANADA, ROGERS'
    END AS NW_CARRIER,
    D_IUM.record_opening_date AS DT,
    D_IUM.SERVE_SID AS SID,
    CAST(D_IUM.SERVED_IMSI AS BIGINT) AS IMSI,
    SUM(0) AS TOT_SMS,
    SUM(0) AS TOT_VOICE_MIN,
    CAST(SUM(NVL(DATA_VOLUME_UPLINK_ARCHIVE,0)) AS DECIMAL(32,0)) + CAST(SUM(NVL(DATA_VOLUME_DOWNLINK_ARCHIVE,0)) AS DECIMAL(32,0)) AS DATA_BYTES
  FROM (
     SELECT A.CELL_ID, A.GPRS_CHOICE_MASK_ARCHIVE, A.SERVED_IMSI, A.RECORD_OPENING_DATE,
        Rt.SERVE_SID, A.DATA_VOLUME_UPLINK_ARCHIVE, A.DATA_VOLUME_DOWNLINK_ARCHIVE
     FROM cdrdm.fact_gprs_cdr A
     JOIN cdrdm.stg_route Rt
    ON
    CASE
          WHEN A.wireless_generation IS NULL THEN COALESCE(TRIM(REGEXP_REPLACE(TRIM(A.Cell_ID),'^0*','')),'')
          WHEN A.wireless_generation IN ('3G','2G') AND (A.Cell_Id IS NULL OR TRIM(A.cell_id) ='' OR A.Cell_Id = '0000') THEN TRIM(A.Eutran_cellid)
          WHEN A.wireless_generation IN ('3G','2G') AND (A.Cell_Id IS NOT NULL AND TRIM(A.cell_id) <> '' AND A.Cell_Id <> '0000') THEN CONCAT(SUBSTR('00000000',1,8-LENGTH(Cell_Id)), TRIM(Cell_ID))
          WHEN A.wireless_generation IN ('4G','5G') AND (A.Eutran_cellid IS NULL OR TRIM(A.Eutran_cellid) =''  OR A.Eutran_cellid = '00000000') THEN CONCAT(SUBSTR('00000000',1,8-LENGTH(A.Cell_Id)), TRIM(A.Cell_ID))
          WHEN A.wireless_generation IN ('4G','5G') AND (A.Eutran_cellid IS NOT NULL AND TRIM(A.Eutran_cellid) <> '' AND A.Eutran_cellid <> '00000000') THEN TRIM(A.Eutran_cellid)
          ELSE 'N-A'
        END = route_id_hex
WHERE A.GPRS_CHOICE_MASK_ARCHIVE = '18' AND SUBSTR(A.SERVED_IMSI,1,6) IN ('302720', '302370','302820') and a.RECORD_OPENING_DATE BETWEEN '${hiveconf:date_from}' AND '${hiveconf:date_to}' 
   ) D_IUM
  LEFT OUTER JOIN (SELECT IMSI, SP_NAME FROM ela_v21.WHOLESALE_SERVICE_ALL_IMSI) SP
  ON D_IUM.SERVED_IMSI = SP.IMSI
  GROUP BY (CASE WHEN SP.SP_NAME IS NOT NULL OR SP.SP_NAME <> '' THEN UPPER(SP.SP_NAME) ELSE 'CANADA, ROGERS' END),D_IUM.record_opening_date,D_IUM.SERVE_SID,(CAST(D_IUM.SERVED_IMSI AS BIGINT));
