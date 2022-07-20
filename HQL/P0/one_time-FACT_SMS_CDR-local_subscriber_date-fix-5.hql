
-- rename main table to backup
ALTER TABLE cdrdm.fact_sms_cdr RENAME TO cdrdm.fact_sms_cdr_BKUP_20161213;
--------------------------------------------------------------------------------------------------

-- rename TEMP table to make it main
ALTER TABLE cdrdm.fact_sms_cdr_TEMP_fix RENAME TO cdrdm.fact_sms_cdr;
--------------------------------------------------------------------------------------------------
