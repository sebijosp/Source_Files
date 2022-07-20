insert into table hem.CCAP_US_PORT_UTIL_15_FCT partition(date_key) select * from hem.CCAP_US_PORT_UTIL_15_FCT_2 where date_key>= '2020-05-01 00:00:00' and date_key < '2020-06-01 00:00:00';
