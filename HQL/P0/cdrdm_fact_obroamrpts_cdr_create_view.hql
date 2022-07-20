use cdrdm;
drop view if exists vw_fact_obroam_call_corr;
CREATE VIEW IF NOT EXISTS vw_fact_obroam_call_corr as select * from cdrdm.fact_obroamrpts_cdr where callcorrected='Y';
