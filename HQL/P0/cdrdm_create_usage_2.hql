--[Version History]
--0.1 - danchang - 4/06/2016 - latest version
--
USE ods_usage;


INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=2;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=20;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=21;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=22;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=23;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=24;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=25;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=26;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=27;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=28;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=29;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=3;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=30;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=35;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=38;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=4;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=5;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=6;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=7;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=8;

DROP TABLE usage;
ALTER TABLE z_usage_temp RENAME TO usage;