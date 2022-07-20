drop table cdrdm.LTE_DL_Throughput_weekly;
create table cdrdm.LTE_DL_Throughput_weekly(
`Week_End_Date` date,
`AreaType` String,
`AreaDetail` String,
`LTE_DL_Throughput` decimal(10,6)
)
row format delimited 
fields terminated by ','
TBLPROPERTIES ("skip.header.line.count"="1");

drop table cdrdm.LTE_Sessions_5_Mbps_weekly;
create table cdrdm.LTE_Sessions_5_Mbps_weekly(
`Week_End_Date` date,
`AreaType` string,
`AreaDetail` string,
`Percent_>_5Mbps` string
)
row format delimited 
fields terminated by ','
TBLPROPERTIES ("skip.header.line.count"="1");

drop table cdrdm.LTE_Unweighted_Unavailability_weekly;
create table cdrdm.LTE_Unweighted_Unavailability_weekly(
`Week_End_Date` date,
`AreaType` String,
`Location_Code` String,
`LTE_Unavailability` string
)
row format delimited 
fields terminated by ','
TBLPROPERTIES ("skip.header.line.count"="1");

drop table cdrdm.VoLTE_RAN_weekly;
create table cdrdm.VoLTE_RAN_weekly(
`WkEnd` date,
`AreaType` String,
`LocCode` String,
`VoLTE_RAN_Access` string,
`VoLTE_RAN_Drop_Rate` String
)
row format delimited 
fields terminated by ','
TBLPROPERTIES ("skip.header.line.count"="1");

