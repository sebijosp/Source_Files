--Modified by H.Umane on 24/OCT/2016 to add new columns cfnumber_orig, PositionDataRecord, CDR_Type,IMEI,IMSI,positionResult,isCached

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

SET WHERE_clause;

INSERT INTO TABLE cdrdm.FACT_GMPC_CDR PARTITION (position_date)
SELECT
CASE WHEN trafficCase = 'mtlr' THEN 0
WHEN trafficCase = 'molr' THEN 2
WHEN trafficCase = 'lc-mtlr' THEN 5 ELSE -1 END,
CASE WHEN locationType = 'current' THEN 0
WHEN locationType = 'currentOrLast' THEN 1
WHEN locationType = 'last' THEN 2
WHEN locationType = 'lastOrCurrent' THEN 3 ELSE -1 END,
COALESCE(clientID,""),
COALESCE(clientNo,""),
COALESCE(pushURL,""),
COALESCE(pushId,""),
COALESCE(errorcode,0), --errorcode1
CASE WHEN clientType = 'valueAddedServices' THEN 0
WHEN clientType = 'emergencyServices' THEN 1
WHEN clientType = 'plmnOperatorServices' THEN 2
WHEN clientType = 'lawfulInterceptServices' THEN 3 ELSE -1 END,
COALESCE(privacyOverride,0),
CASE WHEN coordinateSystem = 'll' THEN 0
WHEN coordinateSystem = 'utm' THEN 1
WHEN coordinateSystem = 'rt90' THEN 2 ELSE -1 END,
CASE WHEN datum = 'wgs84' THEN 0
WHEN datum = 'bessel1841' THEN 1 ELSE -1 END,
CASE WHEN requestedAccuracy = 'low' THEN 0
WHEN requestedAccuracy = 'medium' THEN 1
WHEN requestedAccuracy = 'high' THEN 2 ELSE -1 END,
COALESCE(requestedhoraccuracymeters,0),
CASE WHEN responseTime = 'lowDelay' THEN 0
WHEN responseTime = 'delayTolerant' THEN 1 ELSE -1 END,
concat(substr(requestedpositiontime,1,4),'-',substr(requestedpositiontime,5,2),'-',substr(requestedpositiontime,7,2),' ',substr(requestedpositiontime,9,2),':',substr(requestedpositiontime,11,2),':',substr(requestedpositiontime,13,2)) as requestedPositionTime,
COALESCE(subclientNo,""),
COALESCE(clientRequestor,""),
COALESCE(clientServiceType,0),
COALESCE(targetms,""),
COALESCE(numberMSC,""),
COALESCE(numberSGSN,""),
concat(substr(positiontime,1,4),'-',substr(positiontime,5,2),'-',substr(positiontime,7,2),' ',substr(positiontime,9,2),':',substr(positiontime,11,2),':',substr(positiontime,13,2)) as positiontime,
COALESCE(levelofconfidence,0),
COALESCE(obtainedhoraccuracy,0),
COALESCE(errorCode1,0), --errorcode2
COALESCE(dialledByMS,""),
COALESCE(numberVLR,""),
CASE WHEN requestBearer = 'cp' THEN 0
WHEN requestBearer = 'up' THEN 1 ELSE -1 END,
COALESCE(SetID,""),
COALESCE(latitude,"") as latitude_v,
COALESCE(longitude,"") as longitude_v,
CASE WHEN SUBSTR(latitude,LENGTH(latitude),1) = 'N' THEN CAST(SUBSTR(latitude,1,2) AS DECIMAL(10,5)) + (CAST(SUBSTR(latitude,4,2) AS DECIMAL(10,5))/60) + (CAST(SUBSTR(latitude,7,4) AS DECIMAL(10,5))/3600) WHEN SUBSTR(latitude,1,1) = 'N' THEN CAST (SUBSTR(latitude,2,2) AS DECIMAL(10,5)) + (CAST(SUBSTR(latitude,4,2) AS DECIMAL(10,5))/60 ) + (CAST(SUBSTR(latitude,6,2) AS DECIMAL(10,5))/3600) ELSE 0 END  as latitude,
CASE WHEN SUBSTR(longitude,LENGTH(longitude),1) = 'W' THEN CAST(SUBSTR(longitude,1,3) AS DECIMAL(10,5)) + (CAST(SUBSTR(longitude,5,2) AS DECIMAL(10,5))/60 ) + (CAST(SUBSTR(longitude,8,5) AS DECIMAL(10,5))/3600) WHEN SUBSTR(longitude,1,1) = 'W' THEN CAST (SUBSTR(longitude,2,3) AS DECIMAL(10,5)) + (CAST(SUBSTR(longitude,5,2) AS DECIMAL(10,5))/60 ) + (CAST(SUBSTR(longitude,7,2) AS DECIMAL(10,5))/3600) ELSE 0 END as longitude, 
COALESCE(MAX(province_id),0) AS province_id,
CASE WHEN network = 'unknown' THEN 0
WHEN network = 'network2G' THEN 1
WHEN network = 'network3G' THEN 2
WHEN network = 'lte' THEN 3 
WHEN network = 'nr' THEN 4 ELSE -1 END,
CASE WHEN usedlocationmethod = 'unknown' THEN 0
WHEN usedlocationmethod = 'cgi' THEN 1
WHEN usedlocationmethod = 'utdoa' THEN 2
WHEN usedlocationmethod = 'agps' THEN 3
WHEN usedlocationmethod = 'gps' THEN 4
WHEN usedlocationmethod = 'e-cid' THEN 5
WHEN usedlocationmethod = 'otdoa' THEN 6
WHEN usedlocationmethod = 'agnss' THEN 9
WHEN usedlocationmethod = 'gnss' THEN 10 ELSE -1 END,
COALESCE(esrd,""),
COALESCE(numberMME,""),
COALESCE(numberAMF,""),
COALESCE(callDuration,""),
file_name,
record_start,
record_end,
record_type,
family_name,
version_id,
file_time,
file_id,
switch_name,
num_records,
from_unixtime(unix_timestamp()),
--GMPC MPS changes START
if(INSTR(singlepositions, 'cfnumber') > 0, substr(singlepositions, INSTR(singlepositions, 'cfnumber'), INSTR(substr(singlepositions, INSTR(singlepositions, 'cfnumber')), ',')-1),NULL) AS cfnumber_orig,
trim(PositionDataRecord) AS PositionDataRecord,
--GMPC FIX BEGINS
if(INSTR(PositionDataRecord, 'positionResult :') > 0, 'P',if(INSTR(PositionDataRecord, 'emergencyPushLocationReporting : callRelease :') > 0, 'R','O')) AS CDR_Type,
--GMPC FIX ENDS
IMEI,
IMSI,
positionResult,
isCached,
--GMPC MPS changes END
concat(substr(positionTime,1,4),'-',substr(positionTime,5,2),'-',substr(positionTime,7,2)) as position_date
FROM cdrdm.gmpc a, cdrdm.dim_gmpc_location ${hiveconf:WHERE_clause} 
GROUP BY trafficCase,locationType,clientID,clientNo,pushURL,pushId,errorcode,clientType,privacyOverride,coordinateSystem,datum,requestedAccuracy,requestedhoraccuracymeters,
responseTime,requestedPositionTime,subclientNo,clientRequestor,clientServiceType,targetMS,numberMSC,numberSGSN,positionTime,levelOfConfidence,obtainedhoraccuracy,errorcode1,dialledByMS,
numberVLR,requestBearer,SetID,latitude,longitude,network,usedlocationmethod,esrd,numberMME,numberAMF,callDuration,file_name,record_start,record_end,record_type,family_name,version_id,
file_time,file_id,switch_name,num_records,singlepositions,PositionDataRecord,IMEI,IMSI,positionResult,isCached;

msck repair table cdrdm.fact_gmpc_cdr;
