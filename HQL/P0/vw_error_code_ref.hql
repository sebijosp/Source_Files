CREATE VIEW IPTV.VW_ERROR_CODES_REF AS
  select  column_name, error_description,error_code,source_system,source_field,comments,technical_owner,business_owner 
  from iptv.error_codes_ref;
