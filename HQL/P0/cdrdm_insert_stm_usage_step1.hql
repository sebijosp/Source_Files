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

DROP TABLE IF EXISTS CDRDM.STM_USAGE_STEP1;
DROP TABLE IF EXISTS CDRDM.STM_USAGE_STEP1a;
CREATE TABLE CDRDM.STM_USAGE_STEP1a AS
 SELECT A.IMSI AS IMSI,
call_timestamp_date AS DT,
A.CALL_TYPE AS CALL_TYPE,
UPPER(D.PLMN_NAME) AS NW_CARRIER,
CASE WHEN A.first_cell_id IS NULL OR TRIM(A.first_cell_id) ='' THEN A.first_cell_id
     ELSE ( CASE WHEN A.first_cell_id_extension IS NULL OR TRIM(A.first_cell_id_extension) ='' THEN  CONCAT(SUBSTR('00000000',1,8-LENGTH(SUBSTR(TRIM(A.first_cell_id),11,4))), SUBSTR(TRIM(A.first_cell_id),11,4)) ELSE CONCAT(SUBSTR('00000000',1,8-LENGTH(CONCAT(TRIM(A.first_cell_id_extension), SUBSTR(TRIM(A.first_cell_id),11,4)))), TRIM(A.first_cell_id_extension), SUBSTR(TRIM(A.first_cell_id),11,4)) END) END  AS FIRST_CELL_ID_EXT,
A.CHARGEABLE_DURATION,
CASE WHEN A.CALL_TYPE = 6 OR A.CALL_TYPE = 8 THEN 'SMS'
 WHEN A.CALL_TYPE = 2  OR A.CALL_TYPE = 5 THEN 'VOICE'
  ELSE 'N/A' END AS USAGE_TYPE
FROM CDRDM.FACT_GSM_CDR A 
JOIN ela_v21.plmn_settlement_gg D
ON SUBSTR(A.IMSI,1,5) = SUBSTR(D.PLMN_CODE,1,5)
WHERE A.call_timestamp_date BETWEEN '${hiveconf:date_from}' AND '${hiveconf:date_to}' AND
 A.CALL_TYPE IN (2,5,6,8); 

CREATE TABLE CDRDM.STM_USAGE_STEP1 AS
SELECT COALESCE(UPPER(E.SP_NAME),X.NW_CARRIER) as NW_CARRIER,
    X.DT AS DT,
    x.serve_SID as SID,
    X.IMSI as IMSI,
    SUM(X.SMS_COUNT) AS TOT_SMS,
    CAST(SUM(X.VOICE_MIN_COUNT) AS DECIMAL(32,0)) AS TOT_VOICE_MIN,
  CAST(0 AS DECIMAL(32,0)) AS DATA_BYTES
  FROM (
SELECT a.IMSI, a.DT, a.CALL_TYPE,a.USAGE_TYPE,
    IF ((A.CALL_TYPE = 6 OR A.CALL_TYPE = 8),COUNT(*),0) AS SMS_COUNT,
   IF ((A.CALL_TYPE = 2  OR A.CALL_TYPE = 5),CAST(SUM(CEIL(CAST(NVL(A.CHARGEABLE_DURATION,0) AS DECIMAL(8,2))/60)) AS DECIMAL(32,0)),0) AS VOICE_MIN_COUNT,B.SERVE_SID,A.NW_CARRIER
     FROM CDRDM.STM_USAGE_STEP1a A JOIN cdrdm.stg_route B
  ON a.FIRST_CELL_ID_EXT = b.route_id_hex
  GROUP BY A.IMSI,a.dt,B.SERVE_SID,a.CALL_TYPE,a.USAGE_TYPE,A.NW_CARRIER
  ) X LEFT OUTER JOIN  ela_v21.WHOLESALE_SERVICE_ALL_IMSI E
  ON E.IMSI = x.IMSI
GROUP BY COALESCE(UPPER(E.SP_NAME),X.NW_CARRIER),X.DT,X.serve_SID,X.IMSI;

DROP TABLE  CDRDM.STM_USAGE_STEP1a;
