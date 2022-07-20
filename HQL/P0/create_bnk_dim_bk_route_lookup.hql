CREATE TABLE `cdrdm.bnk_dim_bk_route_lookup`(
  `route_name` varchar(100),
  `carrier` varchar(100),
  `type` varchar(20),
  `lir` varchar(100),
  `mar` varchar(100))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
