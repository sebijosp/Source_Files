CREATE TABLE iptv.PEM_METRICS_HOURLY(

Event_Date  DATE,
Event_ts  STRING,
UniqueAccounts_Perf_Tune  BIGINT,
Unique_Device_Tune  BIGINT,
UniqueAccounts_With_Errors  BIGINT,
Uniquedevices_With_Errors  BIGINT,
PEM  DECIMAL(20,10)

)partitioned by (Event_Year STRING)

STORED AS ORC;
