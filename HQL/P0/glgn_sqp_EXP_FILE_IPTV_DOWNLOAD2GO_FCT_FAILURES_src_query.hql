select

<SELECT_COLS_PLACEHOLDER>

from (
    Select
        header.uuid as header_uuid,
        header.`timestamp` as header_timestamp,
        device.accountSourceID as device_accountSourceID,
        device.deviceSourceID as device_deviceSourceId,
	sessionId,
        failure.offset as failure_offset,
        failure.errordomain as failure_error_domain,
        failure.errordescription as failure_error_description,
        event_date
    from iptv.download2go_fct fct LATERAL VIEW explode(failures) failuresArray as failure
       where fct.event_date >= date_add(current_date,-7)
    ) exploded_failures
 
