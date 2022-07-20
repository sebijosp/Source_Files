set hive.execution.engine=mr;
set hive.support.quoted.identifiers=none;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx8500m;
