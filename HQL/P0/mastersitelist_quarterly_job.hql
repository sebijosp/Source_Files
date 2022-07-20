set hive.execution.engine=tez;

load data local inpath '${hiveconf:DIR}/MasterSiteList.csv' overwrite into table cdrdm.MasterSiteList_tmp;

insert into table cdrdm.MasterSiteList partition(local_dt = '${hiveconf:hive_date}')
select * from cdrdm.MasterSiteList_tmp;
