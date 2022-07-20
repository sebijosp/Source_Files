ALTER TABLE cdrdm.fact_volte_ldrpts_cdr CHANGE ld_flag volte_ld_flag VARCHAR(12);
ALTER TABLE cdrdm.fact_volte_ldrpts_cdr CHANGE country_of_subscriber country_of_volte_subscriber VARCHAR(28);

ALTER TABLE cdrdm.fact_volte_ldrpts_cdr_temp CHANGE ld_flag volte_ld_flag VARCHAR(12);
ALTER TABLE cdrdm.fact_volte_ldrpts_cdr_temp CHANGE country_of_subscriber country_of_volte_subscriber VARCHAR(28);

!echo "cdrdm.fact_volte_ldrpts_cdr table backed out successfully";
