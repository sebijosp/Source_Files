drop table cdrdm.ENIQ_MasterRef_weekly_data;
create table cdrdm.ENIQ_MasterRef_weekly_data(
`emg` String,
`latitude` decimal(10,6),
`longitude` decimal(10,6),
`category` String,
`province` string,
`city` string,
`sitename` string, 
`planning_area` string,
`LTE_DL_Throughput` decimal(10,6),
`Percent_abv_5Mbps` string,
`LTE_Unavailability` string,
`VoLTE_RAN_Access` string,
`VoLTE_RAN_Drop_Rate` string,
`tower_id` string
)
partitioned by (`Week_End_Date` date)
STORED AS ORC;
