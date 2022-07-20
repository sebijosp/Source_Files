set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.tez.container.size=8192;
set hive.cli.print.header=true;
set hive.resultset.use.unique.column.names=false;
set hive.variable.substitute=true;
set hive.default.fileformat=orc;
set hive.execution.engine=tez;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.optimize.index.filter=false;
set hive.optimize.constant.propagation=false;
set hive.tez.auto.reducer.parallelism=true;


CREATE TABLE IF NOT EXISTS iptv.ignite_app_usage_data(
hhid string,
appname string,
one_month_usage bigint,
two_month_usage bigint,
three_month_usage bigint,
four_month_usage bigint,
six_month_usage bigint,
twelve_month_usage bigint,
last_usage_date date,
load_date string);



insert overwrite table iptv.ignite_app_usage_data
select device.accountsourceid as HHID, appname,
SUM(CASE WHEN event_date between '${hiveconf:one_month_end_date}' and '${hiveconf:start_date}' THEN 1 ELSE 0 END) AS one_month_usage,
SUM(CASE WHEN event_date between '${hiveconf:two_month_end_date}' and '${hiveconf:start_date}' THEN 1 ELSE 0 END) AS two_month_usage,
SUM(CASE WHEN event_date between '${hiveconf:three_month_end_date}' and '${hiveconf:start_date}' THEN 1 ELSE 0 END) AS three_month_usage,
SUM(CASE WHEN event_date between '${hiveconf:four_month_end_date}' and '${hiveconf:start_date}' THEN 1 ELSE 0 END) AS four_month_usage,
SUM(CASE WHEN event_date between '${hiveconf:six_month_end_date}' and '${hiveconf:start_date}' THEN 1 ELSE 0 END) AS six_month_usage,
SUM(CASE WHEN event_date between '${hiveconf:twelve_month_end_date}' and '${hiveconf:start_date}' THEN 1 ELSE 0 END) AS twelve_month_usage,
max(event_date) as last_usage_date,
FROM_UNIXTIME(UNIX_TIMESTAMP(), 'YYYY-MM-dd') as load_date
from IPTV.APP_EVENTS_FCT where
((appaction = "start" and appname = "netflixbkg.ci") OR (appaction = "launchingApp" and appname <> "netflixbkg.ci" )) and device.accountsourceid is not null 
and event_date >= '${hiveconf:twelve_month_end_date}' 
group by device.accountsourceid,appname;
