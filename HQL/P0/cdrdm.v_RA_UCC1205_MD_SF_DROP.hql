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

USE cdrdm;

DROP VIEW IF EXISTS v_RA_UCC1205_MD_SF_DROP;

CREATE VIEW IF NOT EXISTS v_RA_UCC1205_MD_SF_DROP AS
Select X.*,
  Case 
    when X.DROP_type = 'BL' then 'MD'
    when X.DROP_type = 'NB' then 'SF'
    Else 'OTHERS'
  End as Record_type_ind,
  Case 
    When File_Type = 'NB'
      then 'from Smart Filter Drop file'
    Else 'from Main Driver Drop file'
  End Source_file_desc
from fact_UCC_unbillable X;