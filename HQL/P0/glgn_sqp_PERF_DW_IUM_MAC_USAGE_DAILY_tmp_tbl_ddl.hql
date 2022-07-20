Create table IF NOT EXISTS HEM.PERF_DW_IUM_MAC_USAGE_DAILY_SQP_IMP(
    cmmac string,
    cmIpAddress string,
    cmtsIP string,
    cmts string,
    MdIfIndex INT,
    MacDomain string,
    product_code string,
    dtype string,
    SMT string,
    NODE string,
    FWDSEG string,
    CmState string,
    Usage_flag CHAR(1),
    ServiceAppID string,
    ServicedMultiCast string,
    Service_Class string,
    us_bytes_avg  DECIMAL(30,4),
    ds_bytes_avg  DECIMAL(30,4),
    usage_avg     DECIMAL(30,4),
    us_bytes_sum  DECIMAL(30,0),
    ds_bytes_sum  DECIMAL(30,0),
    usage_sum     DECIMAL(30,0),
    NoOfHours     INT,
    hdp_insert_ts TIMESTAMP
  )
  partitioned BY
  (
    DAY string
  )
  stored AS ORC ;
