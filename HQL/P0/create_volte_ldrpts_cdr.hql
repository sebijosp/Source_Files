--[Version History]
--0.1 - jayakumar.kuppuswamy - 8/8/2018 - Creates fact_volte_ldrpts_cdr and fact_volte_ldrpts_cdr_temp

SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=orc;
SET hive.execution.engine=tez;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;
SET hive.exec.parallel=true;
SET hive.exec.compress.output=false;

DROP TABLE IF EXISTS cdrdm.fact_volte_ldrpts_cdr_temp;
CREATE TABLE IF NOT EXISTS cdrdm.fact_volte_ldrpts_cdr_temp(
account_desc                        VARCHAR(40),
product_type_desc                   VARCHAR(30),
platform                            VARCHAR(10),
franchise_name                      VARCHAR(10),
roaming_flag                        VARCHAR(1),
volte_ld_flag                       VARCHAR(12),
call_direction                      VARCHAR(3),
country_of_volte_subscriber         VARCHAR(28),
zone                                VARCHAR(20),
reference_destination               VARCHAR(100),
unique_subscribers                  BIGINT,
connected_calls                     BIGINT,
recorded_minutes                    BIGINT,
last_updated_dt                     VARCHAR(10)
)
PARTITIONED BY (month VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
!echo "cdrdm.fact_volte_ldrpts_cdr_temp table (was dropped if existed and) created successfully";

DROP TABLE IF EXISTS cdrdm.dim_volte_ldrpt_months;
CREATE TABLE IF NOT EXISTS cdrdm.dim_volte_ldrpt_months(
dummy varchar(6)
)
PARTITIONED BY (month VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

DROP TABLE IF EXISTS cdrdm.fact_volte_ldrpts_cdr;
CREATE TABLE IF NOT EXISTS cdrdm.fact_volte_ldrpts_cdr(
account_desc                        VARCHAR(40),
product_type_desc                   VARCHAR(30),
platform                            VARCHAR(10),
franchise_name                      VARCHAR(10),
roaming_flag                        VARCHAR(1),
volte_ld_flag                       VARCHAR(12),
call_direction                      VARCHAR(3),
country_of_volte_subscriber         VARCHAR(28),
zone                                VARCHAR(20),
reference_destination               VARCHAR(100),
unique_subscribers                  BIGINT,
connected_calls                     BIGINT,
recorded_minutes                    BIGINT,
last_updated_dt                     VARCHAR(10)
)
PARTITIONED BY (month VARCHAR(10))
STORED AS ORC TBLPROPERTIES("orc.compress"="SNAPPY");
ALTER TABLE cdrdm.fact_volte_ldrpts_cdr SET SERDEPROPERTIES ('serialization.encoding'='UTF-8');
!echo "cdrdm.fact_volte_ldrpts_cdr table (was dropped if existed and) created successfully";
