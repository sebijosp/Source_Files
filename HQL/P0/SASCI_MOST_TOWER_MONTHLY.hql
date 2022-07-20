  
drop table if exists cdrdm.most_tower;
create table if not exists cdrdm.most_tower as
select
	a.subscriber_no,
	a.erp_site_location_code,
	a.tower_cnt,
	b.total_tower_cnt,
	a.tower_cnt/b.total_tower_cnt as tower_pert,
	c.erp_site_name,
	c.x,
	c.y,
	c.address,
	c.city,
	c.province
from
	(select
	*
from
	(select
		*,
		ROW_NUMBER() OVER(PARTITION BY subscriber_no ORDER BY tower_cnt DESC) as tower_rnk
	from
		(select 
			subscriber_no,
			erp_site_location_code,
			count(*) as tower_cnt 
		from 
			(select 
				* 
			from 
				(select 
					record_opening_date, 
					subscriber_no, 
					eutran_cellid, 
					record_opening_time, 
					plmn_id, 
					tracking_area_code
				from cdrdm.fact_gprs_cdr 
					where record_opening_date between '${hiveconf:START_DATE}' and '${hiveconf:END_DATE}' 
					and gprs_choice_mask_archive = 21 
					and eutran_cellid is not null and trim(eutran_cellid) <> '00000000'                        
				) as aa 
			LEFT JOIN 
				(select 
					cgi_hex, 
					cgi, 
					locationco as erp_site_location_code, 
					site_name as erp_site_name, 
					cell as cellmapname
				from 
					(select 
						*, 
						ROW_NUMBER() OVER(PARTITION BY cgi ORDER BY insert_timestamp DESC) as latest 
					from 
						cdrdm.dim_cell_site_info
					) as aa 
					where aa.latest=1 
					and cgi is not null
				) as bb
			ON concat(substr(aa.plmn_id,0,3),'-',substr(aa.plmn_id,-3),'-',conv(aa.tracking_area_code,16,10),'-',conv(aa.eutran_cellid,16,10)) = trim(bb.cgi)
			) as rllup 
		group by 
			subscriber_no,
			erp_site_location_code
		) as fnl_rollup
	) as top_rollup
where
	tower_rnk = 1) a
join
	(select 
	subscriber_no,
	count(*) as total_tower_cnt 
from 
	(select 
		* 
	from 
		(select 
			record_opening_date, 
			subscriber_no, 
			eutran_cellid, 
			record_opening_time, 
			plmn_id, 
			tracking_area_code
		from cdrdm.fact_gprs_cdr 
			where record_opening_date between '${hiveconf:START_DATE}' and  '${hiveconf:END_DATE}'
			and gprs_choice_mask_archive = 21 
			and eutran_cellid is not null and trim(eutran_cellid) <> '00000000'                        
		) as aa 
	LEFT JOIN 
		(select 
			cgi_hex, 
			cgi, 
			locationco as erp_site_location_code, 
			site_name as erp_site_name, 
			cell as cellmapname
		from 
			(select 
				*, 
				ROW_NUMBER() OVER(PARTITION BY cgi ORDER BY insert_timestamp DESC) as latest 
			from 
				cdrdm.dim_cell_site_info
			) as aa 
			where aa.latest=1 
			and cgi is not null
		) as bb
	ON concat(substr(aa.plmn_id,0,3),'-',substr(aa.plmn_id,-3),'-',conv(aa.tracking_area_code,16,10),'-',conv(aa.eutran_cellid,16,10)) = trim(bb.cgi)
	) as rllup 
group by 
	subscriber_no) b
on 
	a.subscriber_no = b.subscriber_no
left join
	(select 
		locationco as erp_site_location_code, 
		site_name as erp_site_name, 
		x, 
		y, 
		address, 
		city, 
		province
	from 
		(select 
			*, 
			ROW_NUMBER() OVER(PARTITION BY locationco ORDER BY insert_timestamp DESC) as latest 
		from 
			cdrdm.dim_cell_site_info
		) as aa 
		where aa.latest=1 
		and locationco is not null
	) c 
on 
	a.erp_site_location_code = c.erp_site_location_code;

  


