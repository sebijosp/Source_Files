CREATE VIEW IPTV.VW_LU_WEEK AS
select
week_id,week_number,week_name,week_abbreviation,month_id,quarter_id,year_id,etl_insrt_run_id,etl_insrt_dt,etl_updt_run_id ,etl_updt_dt,datalake_execution_id ,datalake_load_ts   
from app_shared_dim.lu_week;
