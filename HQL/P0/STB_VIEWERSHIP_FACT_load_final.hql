set hive.execution.engine=mr;
set hive.optimize.sort.dynamic.partition=false;
set set mapreduce.task.timeout=3600000;

insert overwrite table stb_viewership.viewership_fact partition(year, month, day)
select
*
FROM ext_stb_viewership.viewership_fact_sqp_imp_merge ;
