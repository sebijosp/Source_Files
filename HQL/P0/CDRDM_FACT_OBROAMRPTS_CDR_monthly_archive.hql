--[Version History]
--0.1 - ajay.khare - 02/16/2021 - fact_obroamrpts_cdr

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

drop table if exists fact_obroamrpts_cdr_bkup;

create table fact_obroamrpts_cdr_1 like fact_obroamrpts_cdr;

INSERT into fact_obroamrpts_cdr_1 PARTITION(cday, service_type, call_distance_type)
select * from fact_obroamrpts_cdr
where callday >= add_months(trunc(current_date,'MM'),-25);

alter table fact_obroamrpts_cdr rename to fact_obroamrpts_cdr_bkup;

alter table fact_obroamrpts_cdr_1 rename to fact_obroamrpts_cdr;

