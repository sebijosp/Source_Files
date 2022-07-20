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

INSERT INTO TABLE cdrdm.fact_volte_ldrpts_cdr PARTITION(MONTH)
SELECT 
    ACCOUNT_DESC,
    PRODUCT_TYPE_DESC,
    PLATFORM,
    FRANCHISE_NAME,
    ROAMING_FLAG,
    LD_FLAG,
    CALL_DIRECTION,
    COUNTRY_OF_SUBSCRIBER,
    ZONE,
    REFERENCE_DESTINATION,
    SUM(UNIQUE_SUBSCRIBERS) AS UNIQUE_SUBSCRIBERS,
    SUM(CONNECTED_CALLS) AS CONNECTED_CALLS,
    SUM(RECORDED_MINUTES) AS RECORDED_MINUTES,
    LAST_UPDATED_DT,
    MONTH 
FROM 
    cdrdm.fact_volte_ldrpts_cdr_temp
GROUP BY 
    ACCOUNT_DESC,
    PRODUCT_TYPE_DESC,
    PLATFORM,
    FRANCHISE_NAME,
    ROAMING_FLAG,
    LD_FLAG,
    CALL_DIRECTION,
    COUNTRY_OF_SUBSCRIBER,
    ZONE,
    REFERENCE_DESTINATION,
    LAST_UPDATED_DT,
    MONTH 
ORDER BY
    ACCOUNT_DESC,
    PRODUCT_TYPE_DESC,
    PLATFORM,
    FRANCHISE_NAME,
    ROAMING_FLAG,
    LD_FLAG,
    CALL_DIRECTION,
    COUNTRY_OF_SUBSCRIBER,
    ZONE,
    REFERENCE_DESTINATION,
    LAST_UPDATED_DT,
    MONTH;
!echo "Inserted into cdrdm.fact_volte_ldrpts_cdr table";

truncate table cdrdm.fact_volte_ldrpts_cdr_temp;
!echo "Truncated cdrdm.fact_volte_ldrpts_cdr_temp table";
