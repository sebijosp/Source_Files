CREATE TABLE HEM.PERF_DW_IUM_MAC_USAGE_WEEKLY
  (
	cmmac string,
	cmIpAddress string,
	cmtsIP string,
	cmts string,
	MdIfIndex int,
	MacDomain string,
	product_code string,
	dtype string,
	SMT string,
	NODE string,
	FWDSEG string,
	CmState string,
	Usage_flag char(1),
	ServiceAppID string,
	ServicedMultiCast string,
	Service_Class string,
	Sub_Count int,
	us_bytes_avg decimal(30,4),
	ds_bytes_avg decimal(30,4),
	usage_avg decimal(30,4),
	us_bytes_sum decimal(30,0),
	ds_bytes_sum decimal(30,0),
	usage_sum decimal(30,0),
	NoOfDays int,
    hdp_insert_ts TIMESTAMP
  )
  partitioned BY
  (
    DAY date
  )
  stored AS ORC ;
