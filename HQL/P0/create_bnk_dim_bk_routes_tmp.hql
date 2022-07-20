CREATE TABLE `cdrdm.bnk_dim_bk_routes_tmp`(
  `routeid` varchar(10),
  `msc_name` varchar(20),
  `mgw_name` varchar(50),
  `route_name` varchar(100),
  `carrier_name` varchar(100),
  `term_switch` varchar(100),
  `dir` varchar(20),
  `type` varchar(20),
  `sub_type` varchar(20),
  `loc_prov` varchar(50),
  `loc_name_full` varchar(100),
  `lir_name` varchar(100),
  `mar_desc` varchar(100),
  `dso` varchar(50),
  `load_date` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ',';
