Insert overwrite TABLE cdrdm.bnk_dim_bk_routes_tmp
select
z.route_id,
z.msc_name,
z.mgw_name,
z.route_name,
z.carrier_name,
z.term_switch,
z.dir,
z.type,
z.sub_type,
z.loc_prov,
z.loc_name_full,
z.lir_name,
z.mar_desc,
z.dso,
z.load_date
from ( select route_id,
msc_name,
mgw_name,
route_name,
carrier_name,
term_switch,
dir,
type,
sub_type,
loc_prov,
loc_name_full,
lir_name,
mar_desc,
dso,
load_date,
row_number() over (partition by route_name order by load_date,route_name,dso,lir_name,mar_desc,carrier_name)
as first_route_occurrence from cdrdm.dim_bk_routes 
where type not in ('CC','Interswitch','VAS','Vmail'))z
where z.first_route_occurrence=1;
