insert into table  cdrdm.bnk_gsm_transit_cdr_tmp partition(call_timestamp_date='${hiveconf:hive_date}')
select
a.exchange_identity,
1,
case when length(regexp_replace(a.calling_party_number,'([A-Z]+)','')) < 10 then calling_party_number
else
substring(regexp_replace(a.calling_party_number,'([A-Z]+)',''),-10) end as calling_party_number,
case when length(regexp_replace(a.called_party_number,'([A-Z]+)','')) < 10 then called_party_number
else
substring(regexp_replace(a.called_party_number,'([A-Z]+)',''),-10) end as called_party_number,
case when  a.cleansed_calling_number ='' then ''
when (length (a.cleansed_calling_number) > 10) and  (substring(a.cleansed_calling_number,1,4) ='1111' ) then substring(substring(regexp_replace(a.cleansed_calling_number,'([A-Z]+)',''),-10),1,3)
when (length (a.cleansed_calling_number) = 10) then substring(substring(regexp_replace(a.cleansed_calling_number,'([A-Z]+)',''),-10),1,3)
when (length (a.cleansed_calling_number) < 10) then ''
when (length (a.cleansed_calling_number) = 11) and substring(a.cleansed_calling_number,1,1) = '1' then substring(substring(regexp_replace(a.cleansed_calling_number,'([A-Z]+)',''),-10),1,3)
else '' end as a_npa,
case when  a.cleansed_calling_number ='' then ''
when (length (a.cleansed_calling_number) > 10) and  (substring(a.cleansed_calling_number,1,4) ='1111' ) then substring(substring(regexp_replace(a.cleansed_calling_number,'([A-Z]+)',''),-10),4,3)
when (length (a.cleansed_calling_number) = 10) then substring(substring(regexp_replace(a.cleansed_calling_number,'([A-Z]+)',''),-10),4,3)
when (length (a.cleansed_calling_number) < 10) then ''
when (length (a.cleansed_calling_number) = 11) and substring(a.cleansed_calling_number,1,1) = '1' then substring(substring(regexp_replace(a.cleansed_calling_number,'([A-Z]+)',''),-10),4,3)
else '' end as a_nxx,
case when  a.cleansed_called_number ='' then ''
else
substring(substring(regexp_replace(a.cleansed_called_number,'([A-Z]+)',''),-10),1,3) end as b_npa,
case when  a.cleansed_called_number ='' then ''
else
substring(substring(regexp_replace(a.cleansed_called_number,'([A-Z]+)',''),-10),4,3) end as b_nxx,
substring(a.redirecting_number,-10) as redirecting_number,
substring(a.`original_called_number`,-10) as original_called_number,
'',
a.cleansed_redirecting_number ,
a.cleansed_original_number,
a.cleansed_called_number,
a.cleansed_calling_number ,
a.cleansed_charged_number ,
a.`call_timestamp`,
a.`chargeable_duration`,
a.reg_seizure_charging_start,
a.`incoming_route_id` ,
a.`outgoing_route_id`,
a.`record_sequence_number`,
a.imsi,
a.subscriptiontype,
a.`clng_pty_ocn` ,
a.`cld_pty_ocn` ,
a.record_type,
case when (length (a.cleansed_calling_number) > 10) and  (substring(a.cleansed_calling_number,1,4) ='1111' ) then 'NORTH AMERICAN'
when (length (a.cleansed_calling_number) = 10) then 'NORTH AMERICAN'
when (length (a.cleansed_calling_number) < 10) then 'N/A'
when (length (a.cleansed_calling_number) = 11) and substring(a.cleansed_calling_number,1,1) = '1' then 'NORTH AMERICAN'
else 'International' end as a_country,
NULL,
a.calling_party_number,
a.called_party_number,
coalesce(b.lir_name,c.lir_name) as lir,
coalesce(b.mar_desc,c.mar_desc) as mar,
coalesce(b.loc_name_full,c.loc_name_full) as location,
coalesce(b.carrier_name,c.carrier_name) as carrier,
coalesce(b.dso,c.dso) as route_size,
b.type as in_type,
c.type as out_type,
b.route_name as in_route_name,
c.route_name as out_route_name
from cdrdm.fact_gsm_transit_cdr a 
left outer join 
cdrdm.bnk_dim_bk_routes_tmp b on substring(a.incoming_route_id,1,length(a.incoming_route_id)-1) = b.route_name
left outer join 
cdrdm.bnk_dim_bk_routes_tmp c on substring(a.outgoing_route_id,1,length(a.outgoing_route_id)-1) = c.route_name
where call_timestamp_date='${hiveconf:hive_date}';
