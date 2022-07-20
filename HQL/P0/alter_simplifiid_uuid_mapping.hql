ALTER TABLE simplifi.simplifiid_uuid_mapping DROP IF EXISTS PARTITION(file_dt='${hiveconf:hive_date}');
