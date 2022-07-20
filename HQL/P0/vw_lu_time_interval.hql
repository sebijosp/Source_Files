CREATE VIEW IPTV.VW_LU_TIME_INTERVAL AS
SELECT 
time_id,time,hour,military_hour,minute,second,ampm,interval_15,interval_30,etl_insrt_run_id,etl_insrt_dt ,etl_updt_run_id,etl_updt_dt,datalake_execution_id ,datalake_load_ts      
from app_shared_dim.lu_time_interval;
