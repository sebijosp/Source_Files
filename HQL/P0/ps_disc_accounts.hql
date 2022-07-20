drop table if exists mt_pricing_stg.aal_disc_accounts;
create table if not exists mt_pricing_stg.aal_disc_accounts
(
ban   bigint,
ban_aal_disc_dollars   double,
load_ts   timestamp,
build_date date
)
stored as orc;

drop table if exists mt_pricing_stg.desj_disc_accounts;
create table if not exists mt_pricing_stg.desj_disc_accounts
(
ban   bigint,
subscriber_no  varchar(20),
desj_pct   double,
load_ts   timestamp,
build_date date
)
stored as orc;

