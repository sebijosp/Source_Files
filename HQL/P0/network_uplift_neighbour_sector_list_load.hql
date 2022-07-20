CREATE TABLE IF NOT EXISTS NW_TOWER_USAGE.NEIGHBOUR_SECTOR_LIST_TEMP(
pr string,
location_code string,
site_name string,
neighbour string
)
row format delimited fields terminated by ','
stored as textfile
tblproperties("skip.header.line.count"="1");

TRUNCATE TABLE NW_TOWER_USAGE.NEIGHBOUR_SECTOR_LIST_TEMP;

LOAD DATA LOCAL INPATH '${hiveconf:file_name}' OVERWRITE INTO TABLE NW_TOWER_USAGE.NEIGHBOUR_SECTOR_LIST_TEMP;


CREATE TABLE IF NOT EXISTS NW_TOWER_USAGE.NEIGHBOUR_SECTOR_LIST(
pr string,
location_code string,
site_name string,
neighbour string,
load_date string);

INSERT OVERWRITE TABLE NW_TOWER_USAGE.NEIGHBOUR_SECTOR_LIST
SELECT
    ext.*,
    FROM_UNIXTIME(UNIX_TIMESTAMP(), 'YYYY-MM-dd') as load_date
FROM NW_TOWER_USAGE.NEIGHBOUR_SECTOR_LIST_TEMP ext;
