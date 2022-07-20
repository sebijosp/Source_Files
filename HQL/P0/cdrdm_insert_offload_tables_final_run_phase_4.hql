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
SET hive.exec.max.dynamic.partitions=20000;

INSERT INTO TABLE cdrdm.FACT_GMPC_CDR PARTITION (position_date)
SELECT
trim(trafficcase),
trim(locationtype),
clientid,
clientno,
pushurl,
pushid,
trim(errorcode1),
trim(clienttype),
trim(privacyoverride),
trim(coordinatesystem),
trim(datum),
trim(requestedaccuracy),
trim(requestedaccuracymeters),
trim(responsetime),
requestedpositiontime,
subclientno,
clientrequestor,
trim(clientservicetype),
targetms,
numbermsc,
numbersgsn,
positiontime,
trim(levelofconfidence),
trim(obtainedaccuracy),
trim(errorcode2),
dialledbyms,
numbervlr,
trim(requestbearer),
setid,
latitude_v,
longitude_v,
trim(latitude),
trim(longitude),
trim(province_id),
trim(network),
trim(usedlocationmethod),
--trim(time_key),
--trim(audit_key),
esrd,
NumberMME,
trim(CallDuration),
'FACT_GMPC_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gmpc', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr(positiontime,1,10) as position_date
FROM ext_cdrdm.FACT_GMPC_CDR a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GLENAYRE_CDR PARTITION (file_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
record_type,
access_number,
collection_time,
return_code,
service_type,
encoder_type,
capcode,
trim(coverage_region),
trim(call_count),
trim(character_count),
trim(vm_storage_seconds),
trim(meetme_connect_seconds),
customer_enabled,
customer_absent,
--trim(time_key),
switch_id,
--trim(engineering_date_id),
--trim(audit_key),
'FACT_GLENAYRE_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'glenayre', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date as insert_timestamp,
day_date as file_date
FROM ext_cdrdm.FACT_GLENAYRE_CDR a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GLENAYRE_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
trim(channel_seizure_time),
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_duration),
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_cd,
call_type,
units_uom,
--trim(time_key),
switch_id,
trim(unbillable_reason_code),
format_code,
--trim(engineering_date_id),
--trim(audit_key),
'FACT_GLENAYRE_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'glenayredrop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GLENAYRE_UNBILLABLE a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.dim_cell_site_info
SELECT
--FILE_NAME VARCHAR(50),
SITE,
CELL,
ENODEB,
CGI,
CGI_HEX,
SITE_NAME,
ORIGINAL_I,
ANTENNA_TY,
AZIMUTH,
BEAMWIDTH,
X,
Y,
LONGITUDE,
LATITUDE,
ADDRESS,
CITY,
PROVINCE,
ARFCNDL,
BCCHNO,
LOCATIONCO,
BSIC,
PRIMARYSCR,
--START_DATE STRING,
--END_DATE STRING,
--INSERT_TIMESTAMP STRING,
--UPDATE_TIMESTAMP STRING,          
--WORKFLOW_RUN_ID DECIMAL(11,0),
if (substr(FILE_NAME,26,1) = '_', substr(FILE_NAME,23,3), substr(FILE_NAME,23,4)) as FILE_TYPE,
INSERT_TIMESTAMP,
if (substr(FILE_NAME,26,1) = '_', concat(substr(FILE_NAME,27,4),'-',substr(FILE_NAME,31,2),'-',substr(FILE_NAME,33,2)), concat(substr(FILE_NAME,28,4),'-',substr(FILE_NAME,32,2),'-',substr(FILE_NAME,34,2))) as file_date
FROM ext_cdrdm.dim_cell_site_info;