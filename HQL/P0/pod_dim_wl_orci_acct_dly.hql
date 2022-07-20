-- Create table with new structure
drop table data_pods.dim_conv_orci_acct_dly purge;
CREATE TABLE data_pods.dim_conv_orci_acct_dly(
orci_addr_id            bigint,
orci_integrated_id      bigint,
src_acct_id             bigint,
orci_indiv_id           bigint,
contact_id              varchar(40),
ecid                    bigint,
hdp_update_ts           timestamp)
tblproperties ("orc.compress"="SNAPPY");



