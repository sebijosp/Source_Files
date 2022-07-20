CREATE VIEW IPTV.VW_LU_MONTH AS
select
month_id,month_number,month_name,month_abbreviation,days_in_month,quarter_id,year_id,etl_insrt_run_id,etl_insrt_dt,etl_updt_run_id,etl_updt_dt,datalake_execution_id,datalake_load_ts      
from app_shared_dim.lu_month;
