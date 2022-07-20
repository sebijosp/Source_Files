--[Version History]
--0.1 - ajay.khare - 12/02/2020 - fact_obroamrpts_cdr

SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=orc;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
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
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

use cdrdm;
INSERT OVERWRITE TABLE fact_obroamrpts_cdr PARTITION(cday, service_type, call_distance_type)
select 
  callday,
  callmonth,
  callstarttime,
  callendtime,
  brand,
  NULL as imsi,
  NULL as imei,
  NULL as msisdn,
  vplmntadigcode,
  visitedcountry,
  vplmnname,
  mccmnc,
  calltype,
  service,
  calledcountry,
  countrycode,
  callednumber,
  dialednumber,
  calldurationsecs,
  calldurationmins,
  datavolume,
  call_type_level_2,
  access_point_name_ni,
  numberofevents,
  servingcellid,
  servinglac,
  insertdate,
  inserttime,
  file_name,
  record_type,
  family_name,
  version_id,
  file_time,
  file_id,
  switch_name,
  callcorrected,
  cday,
  service_type,
  call_distance_type
from fact_obroamrpts_cdr
where 
callday < add_months(trunc(current_date,'MM'),-13);
