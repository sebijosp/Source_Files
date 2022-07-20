set CURR_DATE;set hiveconf:CURR_DATE;
SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=orc;
SET hive.execution.engine=tez;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.exec.orc.default.buffer.size=32768;


alter table certus.ccts_ban_fido drop if exists partition (rundt='${hiveconf:CURR_DATE}');
INSERT OVERWRITE TABLE certus.ccts_ban_fido PARTITION(rundt)
select a.primary_account, from_unixtime(unix_timestamp(substr(a.date_of_complaint,1,10) ,'MM/dd/yyyy'), 'yyyy-MM-dd'), '${hiveconf:CURR_DATE}' 
from certus.data_ccts_ext a
where (lower(a.service) like '%wir%' OR lower(a.product) like '%wir%')
and a.brand = 'Fido';


alter table certus.ccts_ban_smb drop if exists partition (rundt='${hiveconf:CURR_DATE}');
INSERT OVERWRITE TABLE certus.ccts_ban_smb PARTITION(rundt)
select a.primary_account, from_unixtime(unix_timestamp(substr(a.date_of_complaint,1,10) ,'MM/dd/yyyy'), 'yyyy-MM-dd'), '${hiveconf:CURR_DATE}' 
from certus.data_ccts_ext a
where (lower(a.service) like '%wir%' OR lower(a.product) like '%wir%')
and a.brand = 'Rogers Business';


alter table certus.ccts_ban_rogers drop if exists partition (rundt='${hiveconf:CURR_DATE}');
INSERT OVERWRITE TABLE certus.ccts_ban_rogers PARTITION(rundt)
select a.primary_account, from_unixtime(unix_timestamp(substr(a.date_of_complaint,1,10) ,'MM/dd/yyyy'), 'yyyy-MM-dd'), '${hiveconf:CURR_DATE}' 
from certus.data_ccts_ext a
where (lower(a.service) like '%wir%' OR lower(a.product) like '%wir%')
and a.brand = 'Rogers Consumer';
