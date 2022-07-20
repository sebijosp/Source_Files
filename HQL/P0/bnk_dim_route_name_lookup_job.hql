Insert overwrite table cdrdm.bnk_dim_route_name_lookup select distinct route_name from cdrdm.bnk_dim_bk_routes_tmp;
insert overwrite table cdrdm.bnk_dim_bk_route_lookup SELECT  route_name,carrier_name,type,lir_name,mar_desc FROM cdrdm.bnk_dim_bk_routes_tmp;
