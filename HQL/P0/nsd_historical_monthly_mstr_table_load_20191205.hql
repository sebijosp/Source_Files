use cdrdm;
SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=orc;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.execution.engine=tez;
SET hive.tez.container.size=8192;

--************************************************************************************************************
-- Set of Temp tables to calculate below fields for monthly master:
--  1. monthly_ttl_rev 
--  2. avg_nps
--  3. monthly_data_volume
--  4. sub_cnt
--************************************************************************************************************

--------------------------------------------------------------------------------------------------------------
-- PRE COND: dim_cell_site_info
--------------------------------------------------------------------------------------------------------------

drop table if exists ext_cdrdm.cdr_dim_cell_site_info_latest_monthly_hist;
!echo "Commencing temp table cdr_dim_cell_site_info_latest_monthly_hist creation";
create table if not exists ext_cdrdm.cdr_dim_cell_site_info_latest_monthly_hist stored as orc as
               select * from (select *, ROW_NUMBER() OVER(PARTITION BY cgi ORDER BY insert_timestamp DESC) as latest from cdrdm.dim_cell_site_info) as aa where aa.latest=1 and site_name is not null;
!echo "Temp table cdr_dim_cell_site_info_latest_monthly_hist creation successful";
--************************************************************************************************************
-- ***** Calculation of Home Tower - Run Monthly (with record_opening_date of 3 months back).
--************************************************************************************************************


drop table if exists ext_cdrdm.cdr_sub_level_hme_twr_monthly_hist;
!echo "Commencing temp table ext_cdrdm.cdr_sub_level_hme_twr_monthly_hist creation";
create table if not exists ext_cdrdm.cdr_sub_level_hme_twr_monthly_hist stored as orc as
select * from (
select *, ROW_NUMBER() OVER(PARTITION BY subscriber_no ORDER BY hme_tower DESC) as home_tower from (
select
subscriber_no, 
locationco, 
site_name,
cgi,
count(*) as hme_tower from (
select * from (
select * from 
(select record_opening_date, 
subscriber_no, eutran_cellid, record_opening_time, plmn_id, tracking_area_code,
ROW_NUMBER() OVER(PARTITION BY record_opening_date, subscriber_no ORDER BY record_opening_time DESC) as last_tower_b4_sleep
from cdrdm.fact_gprs_cdr          
where record_opening_date >= '${hiveconf:back_3_mnth_dt}' and record_opening_date <= '${hiveconf:month_end_date}'
and gprs_choice_mask_archive = 21 and eutran_cellid is not null and trim(eutran_cellid) <> '00000000') as aa 
where aa.last_tower_b4_sleep = 1) as aa 
INNER JOIN ext_cdrdm.cdr_dim_cell_site_info_latest_monthly_hist as bb
ON concat(substr(aa.plmn_id,0,3),'-',substr(aa.plmn_id,-3),'-',conv(aa.tracking_area_code,16,10),'-',conv(aa.eutran_cellid,16,10)) = trim(bb.cgi))
as rllup group by subscriber_no, locationco, site_name, cgi order by hme_tower) as fnl_rollup) as fnl_rup where fnl_rup.home_tower = 1
;
!echo "Temp table ext_cdrdm.cdr_sub_level_hme_twr_monthly_hist creation successful";

--************************************************************************************************************
-- ***** Customer Usage - run monthly with range of 1 month.
--************************************************************************************************************
	
drop table if exists ext_cdrdm.cdr_base_cstmr_usge_orc_monthly_hist;
!echo "Commencing temp table ext_cdrdm.cdr_base_cstmr_usge_orc_monthly_hist creation";
create table if not exists ext_cdrdm.cdr_base_cstmr_usge_orc_monthly_hist stored as orc as
select 
'Data_session' as event_type, 
aa.record_opening_date as record_opening_date, 
aa.record_opening_time as record_opening_time, 
aa.subscriber_no as subscriber_no, 
cast(aa.served_imsi as bigint) as imsi,
aa.data_volume_uplink_archive as data_uplink_volume, 
aa.data_volume_downlink_archive as data_donwlink_volume, 
aa.duration as duration, 
aa.eutran_cellid as cell_id, 
bb.* 
from 
(select 
record_opening_date, 
record_opening_time, 
subscriber_no, 
served_imsi,
eutran_cellid,
plmn_id,
tracking_area_code,
max(data_volume_uplink_archive) as data_volume_uplink_archive,  
max(data_volume_downlink_archive) as data_volume_downlink_archive, 
max(duration) as duration
from cdrdm.fact_gprs_cdr where record_opening_date >= '${hiveconf:month_start_date}' and record_opening_date <= '${hiveconf:month_end_date}'
and gprs_choice_mask_archive = 21 
and (spid = '0001' OR spid = '0002') 
and wireless_generation in ('4G', '3G') 
and eutran_cellid is not null and trim(eutran_cellid) <> '00000000'
group by
record_opening_date,
record_opening_time,
subscriber_no,
served_imsi,
eutran_cellid,
plmn_id,
tracking_area_code
) as aa  LEFT JOIN ext_cdrdm.cdr_dim_cell_site_info_latest_monthly_hist as bb ON concat(substr(aa.plmn_id,0,3),'-',substr(aa.plmn_id,-3),'-',conv(aa.tracking_area_code,16,10),'-',conv(aa.eutran_cellid,16,10)) = trim(bb.cgi)
--select 'Data_session' as event_type, aa.record_opening_date as record_opening_date, aa.record_opening_time as record_opening_time, 
--aa.subscriber_no as subscriber_no, 
--cast(aa.served_imsi as bigint) as imsi,
--aa.data_volume_uplink_archive as data_uplink_volume, aa.data_volume_downlink_archive as data_donwlink_volume, aa.duration as duration, aa.eutran_cellid as cell_id, bb.* from 
--(select * from cdrdm.fact_gprs_cdr where record_opening_date >= '${hiveconf:month_start_date}' and record_opening_date <= '${hiveconf:month_end_date}'
--and gprs_choice_mask_archive = 21 
--and (spid = '0001' OR spid = '0002') 
--and wireless_generation in ('4G', '3G') 
--and eutran_cellid is not null and trim(eutran_cellid) <> '00000000'
--) as aa  LEFT JOIN ext_cdrdm.cdr_dim_cell_site_info_latest_monthly_hist as bb ON --concat(substr(aa.plmn_id,0,3),'-',substr(aa.plmn_id,-3),'-',conv(aa.tracking_area_code,16,10),'-',conv(aa.eutran_cellid,16,10)) = trim(bb.cgi)
;
insert into ext_cdrdm.cdr_base_cstmr_usge_orc_monthly_hist
select 'Voice_session' as event_type, cc.call_timestamp_date as record_opening_date, substr(trim(cc.call_timestamp),-8) as record_opening_time, 
cc.subscriber_no as subscriber_no,
cc.imsi as imsi, 
null as data_uplink_volume, null as data_donwlink_volume, cc.chargeable_duration as duration, cc.first_cell_id as cell_id, dd.* from 
(select * from cdrdm.fact_gsm_cdr 
where call_timestamp_date >= '${hiveconf:month_start_date}' and call_timestamp_date <= '${hiveconf:month_end_date}'
and chargeable_duration > 0 
and first_cell_id is not null 
and IMSI >= 302370000000000 and IMSI <=302720999999999
and subscriber_no > 1000000000
) as cc LEFT JOIN ext_cdrdm.cdr_dim_cell_site_info_latest_monthly_hist as dd ON trim(cc.first_cell_id) = trim(dd.cgi_hex)
;
insert into ext_cdrdm.cdr_base_cstmr_usge_orc_monthly_hist 
select 'LTE Voice Session' as event_type, to_date(srvcrequesttimestamp) as record_opening_date, substr(trim(srvcrequesttimestamp),-8) as record_opening_time, 
subscriberno as subscriber_no,
cast(imsi as bigint) as imsi, 
null as data_uplink_volume, null as data_donwlink_volume, chargeableduration as duration, lst1accessnetworkinfo as cell_id, ff.* from 
(select *,
if(length(trim(lst1accessnetworkinfo)) < 20,
concat(substr(lst1accessnetworkinfo,0,3),substr(lst1accessnetworkinfo,5,3),substr(lst1accessnetworkinfo,9,4),substr(lst1accessnetworkinfo,14,length(trim(lst1accessnetworkinfo)))),
concat(substr(lst1accessnetworkinfo,0,3),substr(lst1accessnetworkinfo,5,3),substr(lst1accessnetworkinfo,9,4),substr(lst1accessnetworkinfo,15,length(trim(lst1accessnetworkinfo))))) as new_node
from cdrdm.fact_imm_cdr 
where srvcrequestdttmnrml_date >='${hiveconf:month_start_date}' and srvcrequestdttmnrml_date <='${hiveconf:month_end_date}'
and chargeableduration > 0 
and lst1accessnetworkinfo is not null 
and IMSI >= 302370000000000 and IMSI <=302720999999999
) as ee LEFT JOIN ext_cdrdm.cdr_dim_cell_site_info_latest_monthly_hist as ff ON trim(ee.new_node) = trim(ff.cgi_hex)
;

!echo "Temp table ext_cdrdm.cdr_base_cstmr_usge_orc_monthly_hist load successful";
--************************************************************************************************************
--***** Count number of customers per tower and per month end.
--************************************************************************************************************
	
drop table if exists ext_cdrdm.cdr_base_cstmr_usge_sub_cnt_rolledup_monthly_hist;
!echo "Commencing temp table ext_cdrdm.cdr_base_cstmr_usge_sub_cnt_rolledup_monthly_hist creation";
create table if not exists ext_cdrdm.cdr_base_cstmr_usge_sub_cnt_rolledup_monthly_hist stored as orc as
select 
date_add(last_day(add_months(record_opening_date, -1)),1) as month_ending,
locationco,
count(distinct subscriber_no) as sub_cnt
from ext_cdrdm.cdr_base_cstmr_usge_orc_monthly_hist
group by date_add(last_day(add_months(record_opening_date, -1)),1),locationco;

!echo "Temp table ext_cdrdm.cdr_base_cstmr_usge_sub_cnt_rolledup_monthly_hist creation successful";

--************************************************************************************************************
--*****Join Customer Data with Home Tower Data and pick up ecid from hash_lookup.ecid_uuid_ban_can
--************************************************************************************************************

drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_monthly_hist;
!echo "Commencing temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_monthly_hist creation";
create table if not exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_monthly_hist stored as orc as
select cstmr_hme_twr.*, ecid_sub_map.ecid from 
(select customer.*,
hme_twr.locationco as hme_twr_guess, hme_twr.site_name as hme_twr_site_name  from 
(select * from ext_cdrdm.cdr_base_cstmr_usge_orc_monthly_hist where subscriber_no is not NULL) as customer
LEFT JOIN (select * from ext_cdrdm.cdr_sub_level_hme_twr_monthly_hist where subscriber_no is not NULL and locationco is not NULL) as hme_twr 
ON customer.subscriber_no = hme_twr.subscriber_no) as cstmr_hme_twr
LEFT JOIN (select * from (select subscriber_no, ecid, ROW_NUMBER() OVER(PARTITION BY subscriber_no ORDER BY ecid DESC) as latest_ecid 
from hash_lookup.ecid_uuid_ban_can where subscriber_no is not null) as aa where aa.latest_ecid = 1) as ecid_sub_map 
ON cstmr_hme_twr.subscriber_no = ecid_sub_map.subscriber_no;

!echo "Temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_monthly_hist creation successful";
--************************************************************************************************************
--*****Join cdr_cstmr_hme_twr_ecid_monthly_hist with UCAR (on ecid) to get msf and nps
--************************************************************************************************************
SET hive.execution.engine=mr;
drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly1_hist;
!echo "Commencing temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly1_hist creation";
create table if not exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly1_hist stored as orc as
select customer.*,
date_add(last_day(add_months(record_opening_date, -1)),1) as month_ending,
ucar_dim.voice_msf_revenue_3,
ucar_dim.data_msf_revenue_3,
ucar_dim.avg_nps_rogers_wireless as avg_nps
from 
(select * from ext_cdrdm.cdr_cstmr_hme_twr_ecid_monthly_hist) as customer
LEFT JOIN (select voice_msf_revenue_3, data_msf_revenue_3, avg_nps_rogers_wireless, rundt, ecid from ucar.ucar_final where rundt in
(select max(ucar_latest.rundt) from ucar.ucar_final ucar_latest where ucar_latest.rundt >= '${hiveconf:month_start_date}' and ucar_latest.rundt <= '${hiveconf:month_end_date}')) 
as ucar_dim ON customer.ecid = ucar_dim.ecid;
!echo "Temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly1_hist creation successful";
--************************************************************************************************************
--*****Join cdr_cstmr_hme_twr_ecid_monthly_hist with MLMODEL (on ecid) to get churn score
--************************************************************************************************************
!echo "Commencing temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly_hist creation";
SET hive.execution.engine=tez;
drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly_hist;
create table if not exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly_hist stored as orc as
select customer.*,
churn.model_score as model_score
from 
(select * from ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly1_hist) as customer
LEFT JOIN (select customer_id,model_name, model_level, run_date,model_output_value as model_score  from mlmodel.ml_model_output_fct where model_name = 'RWI_RET_VC_PORT' and model_level = 'ECID' and run_date in
(select max(ml_model_latest.run_date) from mlmodel.ml_model_output_fct ml_model_latest where ml_model_latest.run_date >= '${hiveconf:month_start_date}' and ml_model_latest.run_date <= '${hiveconf:month_end_date}')) as churn on customer.ecid = churn.customer_id
;
!echo "Temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly_hist creation successful";
--************************************************************************************************************
--Aggregation : Aggregation at the ECID, sitename and local_dt level to calculate MSF values per tower 
--               based on the minutes usages on per tower.
--************************************************************************************************************

drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_monthly_hist;
!echo "Commencing temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_monthly_hist creation";
create table if not exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_monthly_hist stored as orc AS
SELECT
month_ending,
ecid,
sum(duration) as mou,
(sum(data_uplink_volume) + sum(data_donwlink_volume)) as data_volume,
avg(voice_msf_revenue_3) as avg_voice_msf_revenue_3,
avg(data_msf_revenue_3) as avg_data_msf_revenue_3
FROM ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly_hist
GROUP BY month_ending, ecid;
!echo "Temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_monthly_hist creation successful";
--************************************************************************************************************
--************************************************************************************************************

drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_monthly_hist;
!echo "Commencing temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_monthly_hist creation";
create table if not exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_monthly_hist stored as orc AS
SELECT
  a.ecid as ecid,
  a.locationco as locationco,
  a.month_ending as month_ending,
  ((a.avg_voice_msf_revenue_3/3) + (a.avg_data_msf_revenue_3/3)) as amt,
  ((a.avg_voice_msf_revenue_3/3) + (a.avg_data_msf_revenue_3/3))*a.mou/b.mou as amt_fraction,
  a.avg_nps_wireless as avg_nps,
  a.mou as mou,
  b.mou as total_mou,
  a.data_volume as data_volume,
  a.mou/b.mou as mou_ratio,
  a.model_score as model_score
  FROM
(SELECT
  ecid,
  locationco,
  month_ending,
  sum(duration) as mou,
  avg(voice_msf_revenue_3) as avg_voice_msf_revenue_3,
  avg(data_msf_revenue_3) as avg_data_msf_revenue_3,
  avg(avg_NPS) as avg_nps_wireless,
  avg(model_score) as model_score,
  (sum(data_uplink_volume) + sum(data_donwlink_volume)) as data_volume
  FROM ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly_hist
  GROUP BY ecid, locationco, month_ending) a
INNER JOIN
  ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_monthly_hist b
  ON a.ecid = b.ecid and a.month_ending = b.month_ending;  

!echo "Temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_monthly_hist creation successful";
--************************************************************************************************************
--Create Rolled Up version of cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_monthly_hist
--************************************************************************************************************
drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_rolledup_monthly_hist;
!echo "Commencing temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_rolledup_monthly_hist creation";
create table if not exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_rolledup_monthly_hist stored as orc as
select 
locationco, 
month_ending, 
avg(avg_nps) as aggr_avg_nps, 
sum(amt_fraction) as aggr_MSF,
sum(data_volume) as monthly_data_volume,
avg(model_score) as churn_score 
from ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_monthly_hist
group by locationco, month_ending;

!echo "Temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_rolledup_monthly_hist creation successful";		
--------------------------------------------------------------------------------------------------------------
--************************************************************************************************************
--Creation of temp table for count/percentage of promoters and detractors 
--************************************************************************************************************
drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_rolledup_monthly1_hist;
!echo "Commencing temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_rolledup_monthly1_hist creation";
create table if not exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_rolledup_monthly1_hist stored as orc as
select 
month_ending,
locationco, 
sum(CASE WHEN avg_nps > 0 and avg_nps < 7 then 1 else 0 END) as cnt_detractors_nps,
sum(CASE WHEN avg_nps >= 7 and avg_nps < 8 then 1 else 0 END) as cnt_passive_nps,
sum(CASE WHEN avg_nps >= 8 then 1 else 0 END) as cnt_promoters_nps,
sum(CASE WHEN avg_nps > 0 and avg_nps < 7 then 1 else 0 END) / sum(CASE WHEN avg_nps IS NOT NULL and avg_nps <> 0 then 1 else 0 END)*100 as pct_detractors_nps,
sum(CASE WHEN avg_nps >= 7 and avg_nps < 8 then 1 else 0 END) / sum(CASE WHEN avg_nps IS NOT NULL and avg_nps <> 0 then 1 else 0 END)*100 as pct_passive_nps,
sum(CASE WHEN avg_nps >= 8 then 1 else 0 END) / sum(CASE WHEN avg_nps IS NOT NULL and avg_nps <> 0 then 1 else 0 END)*100 as pct_promoters_nps
from ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly_hist
group by month_ending,locationco;

!echo "Temp table ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_rolledup_monthly1_hist creation successful";	   
---------------------------------------------------------------------------------------------------------------------------------	   
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--************************************************************************************************************
-- Set of Temp tables to calculate below fields for monthly master:
--  1. Port Out
--************************************************************************************************************

--************************************************************************************************************
-- get customers who ported out last month - monthly run
--************************************************************************************************************

drop table if exists ext_cdrdm.port_out_sub_list_monthly_hist;
!echo "Commencing temp table ext_cdrdm.port_out_sub_list_monthly_hist creation";
create table if not exists ext_cdrdm.port_out_sub_list_monthly_hist stored as orc as
select
date_add(last_day(add_months(to_date(trans_dt), -1)),1) as month_ending,
ctn as subscriber_no,
1 as churn_ind,
sum(case when interaction_outcome like '%to Bell%' then 1 else 0 end) as port_bell,
sum(case when interaction_outcome like '%to Telus%' then 1 else 0 end) as port_telus,
sum(case when interaction_outcome like '%to Freedom%' then 1 else 0 end) as port_freedom,
sum(case when interaction_outcome like '%to Videotron%' then 1 else 0 end) as port_videotron,
sum(case when interaction_outcome not like 'Port%' then 0
when interaction_outcome like '%to Bell%' then 0 
when interaction_outcome like '%to Telus%' then 0 
when interaction_outcome like '%to Freedom%' then 0 
when interaction_outcome like '%to Videotron%' then 0 
when interaction_outcome like '%to Rogers%' then 0 
when interaction_outcome like '%to Fido%' then 0 
else 1 end) as port_other,
sum(case when interaction_outcome like '%to Rogers%' then 1 
when interaction_outcome like '%to Fido%' then 1
else 0 end) as port_interbrand
from app_customer_journey.cbu_cust_journey_deact_fct
where event_description = 'Wireless Deact Vol Churn' and 
to_date(concat(year,'-',month,'-',day)) >= '${hiveconf:month_start_date}' and to_date(concat(year,'-',month,'-',day)) <= '${hiveconf:month_end_date}'
group by ctn, date_add(last_day(add_months(to_date(trans_dt), -1)),1);

!echo "Temp table ext_cdrdm.port_out_sub_list_monthly_hist creation successful"; 
--************************************************************************************************************
-- get the home towers of the subscribers who ported out - monthly run
--************************************************************************************************************

drop table if exists ext_cdrdm.sub_hme_twr_port_out_monthly_hist;
!echo "Commencing temp table ext_cdrdm.sub_hme_twr_port_out_monthly_hist creation";
create table if not exists ext_cdrdm.sub_hme_twr_port_out_monthly_hist stored as orc as
select * from (
select a.*, 
b.locationco,
b.site_name,
b.cgi,
b.hme_tower,
b.home_tower
from ext_cdrdm.port_out_sub_list_monthly_hist as a 
LEFT JOIN  ext_cdrdm.cdr_sub_level_hme_twr_monthly_hist as b 
on trim(a.subscriber_no) = CAST(reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex',CAST(b.subscriber_no AS STRING)) AS VARCHAR(64))) as bb where bb.subscriber_no is not null;

!echo "Temp table ext_cdrdm.sub_hme_twr_port_out_monthly_hist creation successful";
--************************************************************************************************************
-- roll up counts of port outs on month_end + tower_id level - monthly run
--************************************************************************************************************

drop table if exists ext_cdrdm.port_out_monthly_rolledup_hist;
!echo "Commencing temp table ext_cdrdm.port_out_monthly_rolledup_hist creation";
create table if not exists ext_cdrdm.port_out_monthly_rolledup_hist stored as orc as
select month_ending, locationco,sum(churn_ind) as cnt_total
from ext_cdrdm.sub_hme_twr_port_out_monthly_hist
group by month_ending, locationco;
--select month_ending, locationco, (count(port_bell) +
--count(port_telus) +
--count(port_freedom) +
--count(port_videotron) +
--count(port_other) +
--count(port_interbrand) ) as cnt_total
--from ext_cdrdm.sub_hme_twr_port_out_monthly_hist
--group by month_ending, locationco;

!echo "Temp table ext_cdrdm.port_out_monthly_rolledup_hist creation successful";
--************************************************************************************************************
-- Load monthly Master Table - monthly
--************************************************************************************************************

!echo "Commencing Load of Monthly MSTR table";  
INSERT OVERWRITE TABLE cdrdm.nsd_master_monthly_rollup_mstr partition (month_ending) 
SELECT 
  mref.tower_id,
  ct.aggr_MSF,
  ltr.cnt_detractors_nps as cnt_detractors_nps,
  ltr.cnt_passive_nps as cnt_passive_nps,
  ltr.cnt_promoters_nps as cnt_promoters_nps,
  ltr.pct_detractors_nps as pct_detractors_nps,
  ltr.pct_passive_nps as pct_passive_nps,
  ltr.pct_promoters_nps as pct_promoters_nps,
  ct.aggr_avg_nps as avg_nps, 
  ct.monthly_data_volume,
  mref.LTE_DL_Throughput,
  cast(regexp_replace(mref.`percent_>_5mbps`,'%','') as double),
  cast(regexp_replace(mref.LTE_Unavailability,'%','') as double),
  cast(regexp_replace(mref.VoLTE_RAN_Access,'%','') as double),
  cast(regexp_replace(mref.VoLTE_RAN_Drop_Rate,'%','') as double),
  ct.locationco,
  mref.emg,
  mref.sitename,
  mref.province,
  mref.latitude,
  mref.longitude,
  mref.city,
  mref.category,
  mref.planning_area,
  lookup.special_site as special_site_list,
  po.cnt_total, 
  scn.sub_cnt,
  ct.churn_score,
  mref.month_end_date
  from cdrdm.ENIQ_MasterRef_monthly_data mref 
  LEFT JOIN ext_cdrdm.port_out_monthly_rolledup_hist po
  on mref.month_end_date = po.month_ending and mref.tower_id = po.locationco 
  LEFT JOIN ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_rolledup_monthly_hist ct
  on mref.month_end_date = ct.month_ending and mref.tower_id = ct.locationco
  LEFT JOIN ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_rolledup_monthly1_hist ltr
  on mref.month_end_date = ltr.month_ending and mref.tower_id = ltr.locationco
  LEFT JOIN ext_cdrdm.cdr_base_cstmr_usge_sub_cnt_rolledup_monthly_hist scn
  on mref.month_end_date = scn.month_ending and mref.tower_id = scn.locationco
  left join cdrdm.nsd_special_site_lookup lookup
  on mref.tower_id = lookup.tower_id
  where mref.month_end_date = date_add(last_day(add_months('${hiveconf:month_end_date}', -1)),1)
 ;

!echo "Load of Monthly MSTR table successful";
--************************************************************************************************************
-- Drop all temp tables - monthly
--************************************************************************************************************

drop table if exists ext_cdrdm.cdr_dim_cell_site_info_latest_monthly_hist;
drop table if exists ext_cdrdm.cdr_sub_level_hme_twr_monthly_hist;
drop table if exists ext_cdrdm.cdr_base_cstmr_usge_orc_monthly_hist;
drop table if exists ext_cdrdm.cdr_base_cstmr_usge_sub_cnt_rolledup_monthly_hist;
drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_monthly_hist;
drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly_hist;
drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_monthly1_hist;
drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_monthly_hist;
drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_monthly_hist;
drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_rolledup_monthly_hist;
drop table if exists ext_cdrdm.cdr_cstmr_hme_twr_ecid_msf_spread_pertwr_rolledup_monthly1_hist;
drop table if exists ext_cdrdm.port_out_sub_list_monthly_hist;
drop table if exists ext_cdrdm.sub_hme_twr_port_out_monthly_hist;
drop table if exists ext_cdrdm.port_out_monthly_rolledup_hist;

!echo "All Temp tables dropped successfully"; 