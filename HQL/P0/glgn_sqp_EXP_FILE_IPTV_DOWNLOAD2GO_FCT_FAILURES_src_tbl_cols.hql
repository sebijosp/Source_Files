	header_uuid,
	if(header_timestamp is null, '', cast(cast(header_timestamp as Timestamp) as String)),
	device_accountSourceID,
	device_deviceSourceID,
	sessionId,
	failure_offset,
	failure_error_domain,
	failure_error_description,
	event_date
