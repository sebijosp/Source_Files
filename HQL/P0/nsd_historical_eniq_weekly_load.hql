set hive.execution.engine=tez;
load data local inpath '${DIR}/LTE_DL_Throughput_W v1.csv' overwrite into table cdrdm.LTE_DL_Throughput_weekly;
load data local inpath '${DIR}/LTE_Sessions_5_Mbps_W v1.csv' overwrite into table cdrdm.LTE_Sessions_5_Mbps_weekly;
load data local inpath '${DIR}/LTE_Unweighted_Unavailability_W v1.csv' overwrite into table cdrdm.LTE_Unweighted_Unavailability_weekly;
load data local inpath '${DIR}/VoLTE_RAN_W v1.csv' overwrite into table cdrdm.VoLTE_RAN_weekly;

!echo "ENIQ Weekly Tables loaded with Historical data";

insert overwrite table cdrdm.ENIQ_MasterRef_weekly_data partition(`Week_End_Date`)
select
aa.emg,
aa.latitude,
aa.longitude,
aa.category,
aa.province,
aa.csd,
aa.`name`,
aa.`cma-er (rogers)`,
bb.`LTE_DL_Throughput`,
bb.`percent_abv_5mbps`,
bb.`LTE_Unavailability`,
bb.`VoLTE_RAN_Access`,
bb.`VoLTE_RAN_Drop_Rate`,
bb.`AreaDetail` as `tower_id`,
bb.`Week_End_Date`
from
(select `emg`,`latitude`,`longitude`,`category`,`LC`,`province`,`csd`,`name`,`cma-er (rogers)` from cdrdm.MasterSiteList
where local_dt in (select max(distinct local_dt) from cdrdm.MasterSiteList)) aa
RIGHT OUTER JOIN
(select
a.`LTE_DL_Throughput` as `LTE_DL_Throughput`,
b.`Percent_>_5Mbps` as `percent_abv_5mbps`,
c.`LTE_Unavailability` as `LTE_Unavailability`,
d.`VoLTE_RAN_Access` as `VoLTE_RAN_Access`,
d.`VoLTE_RAN_Drop_Rate` as `VoLTE_RAN_Drop_Rate`,
a.`AreaDetail` as `AreaDetail`,
a.`Week_End_Date` as `Week_End_Date`
from
(select * from cdrdm.LTE_DL_Throughput_weekly where `areatype` = 'Site')a
LEFT OUTER JOIN
(select * from cdrdm.LTE_Sessions_5_Mbps_weekly where `areatype` = 'Site')b
ON (a.`Week_End_Date` = b.`Week_End_Date` AND a.`AreaDetail` = b.`AreaDetail`)
LEFT OUTER JOIN
(select * from cdrdm.LTE_Unweighted_Unavailability_weekly where `areatype` = 'Site')c
ON (a.`Week_End_Date` = c.`Week_End_Date` AND a.`AreaDetail` = c.`Location_Code`)
LEFT OUTER JOIN
(select * from cdrdm.VoLTE_RAN_weekly where `areatype` = 'Site')d
ON (a.`Week_End_Date` = d.`WkEnd` AND a.`AreaDetail` = d.`LocCode`)
)bb ON (aa.`LC` = bb.`AreaDetail`);

!echo "cdrdm.ENIQ_MasterRef_weekly_data Table loaded successfully with Historical data";