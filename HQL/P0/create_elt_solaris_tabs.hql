DROP TABLE IF EXISTS iptv.DAILY_ACCOUNT_ERRORS;
	CREATE TABLE iptv.DAILY_ACCOUNT_ERRORS (
		dt_daily DATE
		,unique_error_count DECIMAL(6, 0)
		,extractmonth varchar(10)
		) PARTITIONED BY (dt_monthly DATE) 
		STORED AS ORC TBLPROPERTIES (
		'orc.compress' = 'SNAPPY'
		,'orc.row.index.stride' = '50000'
		,'orc.stripe.size' = '536870912'
		);

DROP TABLE IF EXISTS iptv.DAILY_ERRORS;
	CREATE TABLE iptv.DAILY_ERRORS (
		dt_daily DATE
		,error_code_type VARCHAR(50)
		,error_code VARCHAR(50)
		,description VARCHAR(500)
		,errorcount DECIMAL(4, 0)
		,extractmonth varchar(10)
		) PARTITIONED BY (dt_monthly DATE) 
		STORED AS ORC TBLPROPERTIES (
		'orc.compress' = 'SNAPPY'
		,'orc.row.index.stride' = '50000'
		,'orc.stripe.size' = '536870912'
		);

DROP TABLE IF EXISTS iptv.MONTHLY_ACCOUNT_ERRORS;
	CREATE TABLE iptv.MONTHLY_ACCOUNT_ERRORS (
		unique_error_count DECIMAL(6, 0)
		,extractmonth varchar(10)
		) PARTITIONED BY (dt_monthly DATE) 
		STORED AS ORC TBLPROPERTIES (
		'orc.compress' = 'SNAPPY'
		,'orc.row.index.stride' = '50000'
		,'orc.stripe.size' = '536870912'
		);

DROP TABLE IF EXISTS iptv.MONTHLY_TUNE_TYPES;
CREATE TABLE iptv.MONTHLY_TUNE_TYPES (
	tunetype VARCHAR(50)
	,tune_status VARCHAR(50)
	,asset_class VARCHAR(50)
	,latency DECIMAL(20, 1)
	,tunes DECIMAL(20, 1)
	,extractmonth VARCHAR(10)
	) PARTITIONED BY (dt_monthly DATE) 
	STORED AS ORC TBLPROPERTIES (
	'orc.compress' = 'SNAPPY'
	,'orc.row.index.stride' = '50000'
	,'orc.stripe.size' = '536870912'
	);

DROP TABLE IF EXISTS iptv.DAILY_ACTIVE_ACCOUNTS;
	CREATE TABLE iptv.DAILY_ACTIVE_ACCOUNTS (
		dt_daily DATE
		,accountcount DECIMAL(6, 0)
		,devicecount DECIMAL(6, 0)
		,extractmonth varchar(10)
		) PARTITIONED BY (dt_monthly DATE) 
		STORED AS ORC TBLPROPERTIES (
		'orc.compress' = 'SNAPPY'
		,'orc.row.index.stride' = '50000'
		,'orc.stripe.size' = '536870912'
		);

DROP TABLE IF EXISTS iptv.MONTHLY_ACTIVE_ACCOUNTS;
	CREATE TABLE iptv.MONTHLY_ACTIVE_ACCOUNTS (
		accountcount DECIMAL(6, 0)
		,devicecount DECIMAL(6, 0)
		,extractmonth varchar(10)
		) PARTITIONED BY (dt_monthly DATE) 
		STORED AS ORC TBLPROPERTIES (
		'orc.compress' = 'SNAPPY'
		,'orc.row.index.stride' = '50000'
		,'orc.stripe.size' = '536870912'
		);

DROP TABLE IF EXISTS iptv.MONTHLY_ACTIVE_TEST_ACCOUNTS;
	CREATE TABLE iptv.MONTHLY_ACTIVE_TEST_ACCOUNTS (
		testaccountcount DECIMAL(6, 0)
		,testdevicecount DECIMAL(6, 0)
		,extractmonth varchar(10)
		) PARTITIONED BY (dt_monthly DATE) 
		STORED AS ORC TBLPROPERTIES (
		'orc.compress' = 'SNAPPY'
		,'orc.row.index.stride' = '50000'
		,'orc.stripe.size' = '536870912'
		);
		
		
DROP TABLE IF EXISTS iptv.ERRORS_PER_HOUR;		
CREATE TABLE IPTV.ERRORS_PER_HOUR(
		dt_daily DATE
  ,accounts decimal(6,0),
  hours decimal(6,2),
  hourspererroraccounts decimal(6,2),
  total_error_response_codes decimal(6,0),
  errorsperhour decimal(6,2),
  extractmonth varchar(10))	
     PARTITIONED BY (dt_monthly DATE) 
	 STORED AS ORC
   TBLPROPERTIES (
		'orc.compress' = 'SNAPPY'
		,'orc.row.index.stride' = '50000'
		,'orc.stripe.size' = '536870912'
		);		


DROP TABLE IF EXISTS iptv.MONTHLY_ACTIVE_ACC_DEVICE_TYPE;	
CREATE TABLE IPTV.MONTHLY_ACTIVE_ACC_DEVICE_TYPE(
devicetype varchar(50),
accountcount decimal(6,0),
extractmonth varchar(10))
PARTITIONED BY (dt_monthly DATE) 
STORED AS ORC
TBLPROPERTIES (
'orc.compress' = 'SNAPPY'
,'orc.row.index.stride' = '50000'
,'orc.stripe.size' = '536870912'
);
