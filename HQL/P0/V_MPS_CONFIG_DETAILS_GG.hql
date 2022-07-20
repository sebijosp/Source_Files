DROP VIEW IF EXISTS ela_v21.V_MPS_CONFIG_DETAILS_GG;
CREATE VIEW ela_v21.V_MPS_CONFIG_DETAILS_GG AS
SELECT HEX(CASE WHEN inp_val_str is null THEN NULL else CAST(TRIM(inp_val_str) as INT) END) AS hex_inp_val_str, inp_val_str, description
FROM ela_v21.MPS_CONFIG_DETAILS_GG WHERE
category_id = 2 AND 
expiration_date > "2098-07-01";