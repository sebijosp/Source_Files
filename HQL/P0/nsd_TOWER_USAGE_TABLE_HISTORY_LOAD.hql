use NW_TOWER_USAGE;
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


INSERT OVERWRITE TABLE NW_TOWER_USAGE.tower_usage partition(RECORD_DATE)
select
ods.customer_ban as ban,
rnk_rup.subscriber_no,
rnk_rup.city,
rnk_rup.province,
case when rnk_rup.stat = 'Cell ID' then Null
else rnk_rup.erp_site_location_code
end as erp_site_location_code,
rnk_rup.record_date
from
(select
*,
ROW_NUMBER() OVER(PARTITION BY subscriber_no ORDER BY work_cnt DESC) as tower_rnk
from
(select
subscriber_no,
case when city is null or city = '' then 'None' else city end as city,
case when province is null or province = '' then 'None' else province end as province,
case when (erp_site_location_code is null or erp_site_location_code = '') then eutran_cellid
else erp_site_location_code end as erp_site_location_code,
case when (erp_site_location_code is null or erp_site_location_code = '') then 'Cell ID'
else 'Location Code' end as stat,
count(*) as work_cnt,
date_sub(current_date,${hiveconf:day}) as record_date
from
(select
subscriber_no,
eutran_cellid,
plmn_id,
tracking_area_code
from cdrdm.fact_gprs_cdr
where record_opening_date between date_add((date_sub(current_date,${hiveconf:day})),-30) and date_sub(current_date,${hiveconf:day})
and gprs_choice_mask_archive = 21
and eutran_cellid is not null and trim(eutran_cellid) <> '00000000'
) as aa
LEFT JOIN
(select
cgi_hex,
cgi,
locationco as erp_site_location_code,
site_name as erp_site_name,
cell as cellmapname,
x,
y,
address,
city,
province
from
(select
*,
ROW_NUMBER() OVER(PARTITION BY cgi ORDER BY insert_timestamp DESC) as latest
from
cdrdm.dim_cell_site_info) as sites
where sites.latest=1
and cgi is not null
) as bb
ON concat(substr(aa.plmn_id,0,3),'-',substr(aa.plmn_id,-3),'-',conv(aa.tracking_area_code,16,10),'-',conv(aa.eutran_cellid,16,10)) = trim(bb.cgi)
group by
subscriber_no,
case when city is null or city = '' then 'None' else city end,
case when province is null or province = '' then 'None' else province end,
case when (erp_site_location_code is null or erp_site_location_code = '') then eutran_cellid
else erp_site_location_code end,
case when (erp_site_location_code is null or erp_site_location_code = '') then 'Cell ID'
else 'Location Code' end
order by
work_cnt
) as rup
) as rnk_rup
join
(select
subscriber_no,
customer_ban
from
(select
subscriber_no,
first_value(customer_ban) over (partition by subscriber_no order by sub_status_date desc) as customer_ban
from
(select 
subscriber_no, 
customer_ban,
sub_status_date
from ods.subscriber
where sub_status in ('A','S')) as ods_inner
) as ods_outer
group by
subscriber_no,
customer_ban) as ods
on rnk_rup.subscriber_no = ods.subscriber_no
where rnk_rup.tower_rnk = 1;