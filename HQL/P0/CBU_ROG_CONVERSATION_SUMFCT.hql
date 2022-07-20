SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

USE verint;

DROP TABLE CBU_ROG_CONVERSATION_SUMFCT;

CREATE TABLE CBU_ROG_CONVERSATION_SUMFCT (
	speech_id_internal BIGINT,
	speech_id_verint String,
	text_agent_full STRING,   
	text_customer_full STRING,   
	text_overlap STRING,   
	text_unknown STRING, 
	text_all STRING,   
	customer_id STRING,   
	ctn STRING,   
	interaction_id STRING,   
	agent_emp_id STRING,
	connection_id STRING,   
	receiving_skill STRING,   
	source_speech_file_name String,
	source_meta_file_name String,
	record_insert_dt TIMESTAMP
) 
PARTITIONED BY (conversation_date DATE)
stored as orc 
tblproperties(  
	'orc.compress'='SNAPPY',
	'orc.row.index.stride'='50000',
	'orc.stripe.size'='536870912',
	'transient.lastDdlTime'='1506095272'
);
