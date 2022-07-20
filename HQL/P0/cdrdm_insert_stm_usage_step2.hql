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

DROP TABLE IF EXISTS CDRDM.STM_USAGE_STEP2;
CREATE TABLE CDRDM.STM_USAGE_STEP2 AS
SELECT
 UPPER(X.OP_NAME) AS NW_CARRIER,
  X.DT,
  X.SERVE_SID AS SID ,
  CAST(X.SERVEDIMSI AS BIGINT) AS IMSI,
  SUM(0) AS TOT_SMS,
  SUM(0) AS TOT_VOICE_MIN,
  CAST(SUM(NVL(ULNK1,0) + NVL(ULNK2,0) + NVL(ULNK3,0) + NVL(ULNK4,0)
  +NVL(ULNK5,0) + NVL(DLNK1,0) + NVL(DLNK2,0) + NVL(DLNK3,0) +
  NVL(DLNK4,0) + NVL(DLNK5,0)) AS DECIMAL(32,0)) AS DATA_BYTES
  FROM (
   SELECT
   D_SGW.recordopeningdate AS DT,
   B.PLMN_NAME AS OP_NAME,
   Rt.SERVE_SID,
   D_SGW.SERVEDIMSI,
   CAST(SUM(dataVolumeGPRSDownlink1) AS DECIMAL(32,0)) AS DLNK1,
   CAST(SUM(dataVolumeGPRSDownlink2)AS DECIMAL(32,0)) AS DLNK2 ,
   CAST(SUM(dataVolumeGPRSDownlink3) AS DECIMAL(32,0)) AS DLNK3,
   CAST(SUM(dataVolumeGPRSDownlink4) AS DECIMAL(32,0))  AS DLNK4,
   CAST(SUM(dataVolumeGPRSDownlink5) AS DECIMAL(32,0))  AS DLNK5,
   CAST(SUM(dataVolumeGPRSUPlink1)AS DECIMAL(32,0))  AS ULNK1,
   CAST(SUM(dataVolumeGPRSUPlink2)AS DECIMAL(32,0))  AS ULNK2,
   CAST(SUM(dataVolumeGPRSUPlink3) AS DECIMAL(32,0)) AS ULNK3,
   CAST(SUM(dataVolumeGPRSUPlink4)AS DECIMAL(32,0))  AS ULNK4,
   CAST(SUM(dataVolumeGPRSUPlink5) AS DECIMAL(32,0)) AS ULNK5
   FROM cdrdm.FACT_GPRS_SGW_CDR D_SGW
   JOIN ela_v21.plmn_settlement_gg B
   ON  SUBSTR(TRIM(B.PLMN_CODE),1,5) = SUBSTR(D_SGW.SERVEDIMSI,1,5)
   JOIN cdrdm.stg_route Rt
   ON D_SGW.ECID_HEX = rt.route_id_hex
WHERE D_SGW.recordopeningdate BETWEEN '${hiveconf:date_from}' AND '${hiveconf:date_to}'
GROUP BY D_SGW.recordopeningdate,B.PLMN_NAME,Rt.SERVE_SID,D_SGW.SERVEDIMSI) X
  GROUP BY UPPER(X.OP_NAME),X.DT,X.SERVE_SID,(CAST(X.SERVEDIMSI AS BIGINT));
