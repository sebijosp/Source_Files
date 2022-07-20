SET hive.variable.substitute=true;
SET hive.auto.convert.join = true;
SET hive.default.fileformat=Orc;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism = true;
SET hive.convert.join.bucket.mapjoin.tez=true;

ALTER TABLE cdrdm.mps_cust_1 RENAME TO cdrdm.dim_mps_cust_1;
ALTER TABLE ext_cdrdm.cell_site_info RENAME TO ext_cdrdm.dim_cell_site_info;
ALTER TABLE ext_cdrdm.cell_site_info_lte RENAME TO ext_cdrdm.dim_cell_site_info_lte;
ALTER TABLE ext_cdrdm.cell_site_info_umts RENAME TO ext_cdrdm.dim_cell_site_info_umts;
ALTER TABLE ext_cdrdm.cell_site_info_gsm RENAME TO ext_cdrdm.dim_cell_site_info_gsm;
ALTER TABLE cdrdm.cell_site_info RENAME TO cdrdm.dim_cell_site_info;
ALTER TABLE cdrdm.time RENAME TO cdrdm.dim_time;
ALTER TABLE cdrdm.state_mps RENAME TO cdrdm.dim_state_mps;
ALTER TABLE cdrdm.gmpc_location RENAME TO cdrdm.dim_gmpc_location;
ALTER TABLE cdrdm.interaction_source RENAME TO cdrdm.dim_interaction_source;
ALTER TABLE cdrdm.carrier RENAME TO cdrdm.dim_carrier;
ALTER TABLE cdrdm.fault_code RENAME TO cdrdm.dim_fault_code;
ALTER TABLE cdrdm.eng_handset_tac RENAME TO cdrdm.dim_eng_handset_tac;
ALTER TABLE cdrdm.operatorcodes_eng RENAME TO cdrdm.dim_operatorcodes_eng;
ALTER TABLE cdrdm.age_group RENAME TO cdrdm.dim_age_group;
ALTER TABLE cdrdm.date RENAME TO cdrdm.dim_date;

DROP TABLE cdrdm.npa_nxx_gg_state_mps; 

CREATE VIEW cdrdm.npa_nxx_gg_dim_state_mps as
SELECT
a.npa, 
a.nxx,
b.country_code
FROM ela_v21.npa_nxx_gg a, cdrdm.dim_STATE_MPS b 
WHERE trim(a.state_code) = trim(b.state_code);