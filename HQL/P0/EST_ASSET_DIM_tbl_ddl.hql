drop table if exists iptv.EST_ASSET_DIM;
create table iptv.EST_ASSET_DIM (
asset_id VARCHAR(20),
provider_name VARCHAR(100),
product_name VARCHAR(100),
asset_name VARCHAR(100),
asset_desc VARCHAR(4000),
creation_ts DATE,
provider_id VARCHAR(100),
package_id VARCHAR(20),
asset_class VARCHAR(20),
season_premiere_cd VARCHAR(10),
season_finale_cd VARCHAR(10),
episode_name VARCHAR(100),
episode_id VARCHAR(20),
eligible_platform_cd VARCHAR(50),
vod_type_cd VARCHAR(20),
provider_content_tier_text VARCHAR(200),
ca_rating_cd VARCHAR(10),
vsp_id int,
asset_type_cd VARCHAR(20),
title_sort_name VARCHAR(100),
title_brief_name VARCHAR(100),
title_display_name VARCHAR(100),
eidr_num VARCHAR(50),
summary_long_text STRING,
summary_medium_text VARCHAR(4000),
summary_short_text VARCHAR(1000),
rating_cd VARCHAR(10),
closed_captioning VARCHAR(10),
run_time VARCHAR(100),
display_run_time VARCHAR(100),
year int,
country_of_origin VARCHAR(100),
actors VARCHAR(1000),
actors_display VARCHAR(1000),
writer_display VARCHAR(100),
director VARCHAR(100),
producers VARCHAR(100),
studio VARCHAR(100),
studio_name VARCHAR(200),
category VARCHAR(1024),
genre VARCHAR(100),
show_type VARCHAR(100),
billing_id VARCHAR(100),
licensing_window_start TIMESTAMP,
licensing_window_end TIMESTAMP,
preview_period VARCHAR(50),
display_as_new int,
display_as_last_chance int,
maximum_viewing_length VARCHAR(100),
contract_name VARCHAR(50),
suggested_price double,
studio_royalty_percent double,
ofrb int,
cavco VARCHAR(10),
program_type VARCHAR(100),
isci VARCHAR(30),
audio_type VARCHAR(50),
screen_format VARCHAR(50),
resolution VARCHAR(100),
frame_rate DOUBLE,
codec VARCHAR(50),
languages VARCHAR(100),
subtitle_languages VARCHAR(100),
dubbed_languages VARCHAR(100),
bit_rate bigint,
content_filesize bigint,
content_checksum VARCHAR(50),
trickmodesrestricted VARCHAR(100),
hdcontent VARCHAR(10),
content_format VARCHAR(100),
content VARCHAR(100),
recordsource VARCHAR(100),
recordtimestamp TIMESTAMP,
correctionreason VARCHAR(100),
provider_qa_contact varchar(255),
distributor_royalty_minimum decimal(32,2),
distributor_royalty_percent decimal(32,2),
distributor_name varchar(1000),
dvs varchar(10),
uhd varchar(10),
hdr varchar(10),
hdr_type varchar(100),
box_office bigint,
msorating varchar(10),
program_name varchar(100), 
is_active_flag varchar(1),
received_date DATE,
hdp_create_ts timestamp,
hdp_update_ts timestamp
)

STORED AS ORC;
