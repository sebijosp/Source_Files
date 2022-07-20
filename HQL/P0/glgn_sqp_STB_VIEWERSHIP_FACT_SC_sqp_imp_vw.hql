CREATE VIEW `ext_stb_viewership.VIEWERSHIP_FACT_SC_sqp_imp` AS select

CAST(if(LOWER(`viewership_fact_sc_src_file`.`can`) = 'null', null, `viewership_fact_sc_src_file`.`can`) AS VARCHAR(20)) AS `CAN`,
CAST(null AS varchar(5)) AS `COMPANY_NUMBER`,
CAST(null AS VARCHAR(15)) AS `ACCOUNT_NUMBER`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`channel`) = 'null', null, `viewership_fact_sc_src_file`.`channel`) AS INT) AS `CHANNEL`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`channel_desc`) = 'null', null, `viewership_fact_sc_src_file`.`channel_desc`) AS VARCHAR(50)) AS `CHANNEL_DESC`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`device_id`) = 'null', null, `viewership_fact_sc_src_file`.`device_id`) AS VARCHAR(40)) AS `DEVICE_ID`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`start_time`) = 'null', null, from_unixtime(unix_timestamp(`viewership_fact_sc_src_file`.`start_time`, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP) AS `START_VIEW`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`end_time`) = 'null', null, from_unixtime(unix_timestamp(`viewership_fact_sc_src_file`.`end_time`, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP) AS `END_VIEW`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`duration_sec`) = 'null', 0, `viewership_fact_sc_src_file`.`duration_sec`) AS INT) AS `DURATION_SEC`,
CAST((CAST(if(LOWER(`viewership_fact_sc_src_file`.`duration_sec`) = 'null', 0, `viewership_fact_sc_src_file`.`duration_sec`) AS INT)/60) AS INT) AS `DURATION_MIN`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`hub_id`) = 'null', null, `viewership_fact_sc_src_file`.`hub_id`) AS INT) AS `HUB_ID`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`ipg_channel`) = 'null', null, `viewership_fact_sc_src_file`.`ipg_channel`) AS VARCHAR(50)) AS `IPG_CHANNEL`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`is_sdv`) = null, null, `viewership_fact_sc_src_file`.`is_sdv`) AS INT) AS `IS_SDV`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`n_hour_end_time`) = 'null', null, from_unixtime(unix_timestamp(`viewership_fact_sc_src_file`.`n_hour_end_time`, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP) AS `N_HOUR_END_TIME`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`node_id`) = 'null', null, `viewership_fact_sc_src_file`.`node_id`) AS VARCHAR(20)) AS `NODE_ID`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`privacy_opt`) = 'null', null, `viewership_fact_sc_src_file`.`privacy_opt`) AS INT) AS `PRIVACY_OPT`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`qpsk_demod_id`) = 'null', null, `viewership_fact_sc_src_file`.`qpsk_demod_id`) AS INT) AS `QPSK_DEMOD_ID`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`qpsk_mod_id`) = 'null', null, `viewership_fact_sc_src_file`.`qpsk_mod_id`) AS INT) AS `QPSK_MOD_ID`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`sam_channel`) = 'null', null, `viewership_fact_sc_src_file`.`sam_channel`) AS VARCHAR(50)) AS `SAM_CHANNEL`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`sgid`) = 'null', null, `viewership_fact_sc_src_file`.`sgid`) AS INT) AS `SGID`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`source_id`) = 'null', null, `viewership_fact_sc_src_file`.`source_id`) AS INT) AS `SOURCE_ID`,
CAST(if(LOWER(`viewership_fact_sc_src_file`.`zip_code`) = 'null', null, `viewership_fact_sc_src_file`.`zip_code`) AS VARCHAR(11)) AS `ZIP_CODE`,

CAST('SC' AS VARCHAR(10)) as `SOURCE_TYPE`,
CAST(split(`viewership_fact_sc_src_file`.`input__file__name`, '/userapps/glagoon/stb_viewership/SC/landing/')[1] as varchar(100)) as `SOURCE_FILENAME`,
year(CAST(if(LOWER(`viewership_fact_sc_src_file`.`start_time`) = 'null', null, from_unixtime(unix_timestamp(`viewership_fact_sc_src_file`.`start_time`, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP)) AS `YEAR`,
month(CAST(if(LOWER(`viewership_fact_sc_src_file`.`start_time`) = 'null', null, from_unixtime(unix_timestamp(`viewership_fact_sc_src_file`.`start_time`, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP)) AS `MONTH`,
day(CAST(if(LOWER(`viewership_fact_sc_src_file`.`start_time`) = 'null', null, from_unixtime(unix_timestamp(`viewership_fact_sc_src_file`.`start_time`, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP)) AS `DAY`

FROM `ext_stb_viewership`.`viewership_fact_sc_src_file`
where CAST(if(LOWER(`viewership_fact_sc_src_file`.`start_time`) = 'null', null, from_unixtime(unix_timestamp(`viewership_fact_sc_src_file`.`start_time`, 'MM/dd/yyyy HH:mm:ss'))) AS TIMESTAMP) is not null;
