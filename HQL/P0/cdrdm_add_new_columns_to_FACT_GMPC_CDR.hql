--Add the new columns under the MPS project to the cdrdm.fact_gmpc_cdr table
ALTER TABLE cdrdm.fact_gmpc_cdr ADD COLUMNS(cfnumber_orig string,PositionDataRecord string,CDR_Type string,IMEI string,IMSI string,positionResult string,isCached string);

