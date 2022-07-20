set hive.execution.engine=tez;
set hive.auto.convert.join=false;
drop table if exists ext_app_tvm.NAC_CALENDAR;
create table if not exists ext_app_tvm.NAC_CALENDAR stored as orc AS 
SELECT  * FROM
(SELECT NAC.*,
ROW_NUMBER () OVER (PARTITION BY BAN, SUBSCRIBER_NO, NAC_YEAR, NAC_MONTH ORDER BY MSP_YEAR DESC, MSP_MONTH DESC ) AS SEQ
FROM APP_TVM.NAC NAC
WHERE CONCAT(MSP_YEAR,'-',LPAD(MSP_MONTH,2,'0'),'-','01') BETWEEN add_months(CONCAT(NAC_YEAR,'-',LPAD(NAC_MONTH,2,'0'),'-','01'),-1) AND add_months(CONCAT(NAC_YEAR,'-',LPAD(NAC_MONTH,2,'0'),'-','01'),+1) and INIT_ACTIVATION_DATE < date_add(last_day(current_date),1) 
  ) X
WHERE SEQ=1 ;
drop table if exists app_tvm.NAC_CALENDAR;
alter table ext_app_tvm.NAC_CALENDAR rename to app_tvm.NAC_CALENDAR;
