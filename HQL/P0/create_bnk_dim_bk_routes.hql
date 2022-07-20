drop table `cdrdm.bnk_dim_bk_routes`;
CREATE TABLE `cdrdm.bnk_dim_bk_routes`(
`routeid` varchar(10),
`msc` varchar(20),
`mgw` varchar(50),
`route_name` varchar(100),
`carrier` varchar(100),
`term_switch` varchar(100),
`dir` varchar(20),
`type` varchar(20),
`sub_type` varchar(20),
`loc_prov` varchar(50),
`location` varchar(100),
`lir` varchar(100),
`mar` varchar(100),
`ds0` varchar(50),
`date_loaded` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
