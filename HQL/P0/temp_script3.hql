insert overwrite table hem.CCAP_US_PORT_UTIL_WEEKLY_FCT Partition(date_key)
 Select * from hem.CCAP_US_PORT_UTIL_WEEKLY_FCT_BD;
