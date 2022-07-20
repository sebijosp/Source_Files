Insert into simplifi.simplifiid_uuid_mapping partition(file_dt='${hiveconf:hive_date}')
select Simplifi_id,UUID,CURRENT_DATE from simplifi.simplifiid_uuid_mapping_ext;
