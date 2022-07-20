create table cdrdm.MasterSiteList(
`Province` String,
`LC` String,
`EMG` String,
`Name` String,
`Category` String,
`CategoryDetail` String,
`Site_Type` String,
`Tower_Type` String,
`Latitude` decimal(10,6),
`Longitude` decimal(10,6),
`Owner` String,
`Company_Owner` String,
`Partner` String,
`Height` decimal(9,2),
`Technologies` String,
`LteCarriers` String,
`UmtsCarriers` String,
`GsmCarriers` String,
`RepeaterCarriers` String,
`CMA-ER (Rogers)` String,
`CSD` String,
`PC` String,
`CD` String,
`CMA` String,
`ER` String,
`EXT` String,
`EXT_LAC` bigint,
`FinalConfig` String
)
partitioned by (local_dt date)
STORED AS ORC;
