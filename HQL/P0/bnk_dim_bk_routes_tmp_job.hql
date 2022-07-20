Insert overwrite TABLE cdrdm.bnk_dim_bk_routes_tmp
select
z.routeid,
z.msc,
z.mgw,
z.route_name,
z.carrier,
z.term_switch,
z.dir,
z.type,
z.sub_type,
z.loc_prov,
z.location,
z.lir,
z.mar,
z.ds0,
z.date_loaded
from ( select routeid,
msc,
mgw,
route_name,
carrier,
term_switch,
dir,
type,
sub_type,
loc_prov,
location,
lir,
mar,
ds0,
date_loaded,
row_number() over (partition by route_name order by date_loaded,route_name,ds0,lir,mar,carrier)
as first_route_occurrence from cdrdm.dim_bk_routes
where type not in ('CC','Interswitch','VAS','Vmail'))z
where z.first_route_occurrence=1;

