CREATE OR REPLACE VIEW HEM.TIMEZONE_ZIPCODE_XREF AS
select 
city_name as CITY,
province_code as PROVINCE,
postal_code as ZIPCODE,
preferred as PREFFERED,
area_code as AREA_CODE,
time_zone as TIMEZONE_NAME,
dst as IS_DST
from app_shared_dim.zipinfo
where preferred = 'P';
