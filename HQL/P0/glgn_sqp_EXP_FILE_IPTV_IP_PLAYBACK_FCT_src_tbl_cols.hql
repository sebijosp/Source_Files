header.uuid,
NULL, -- header.versionNum
if(header.`timestamp` is null, '', cast(cast(header.`timestamp` as Timestamp) as String)),
partnerId, -- header.partner
header.hostname,
eventType,
session.pluginSessionId, -- sessionid
if(startTime is null, '', cast(Cast(startTime as Timestamp) as String)),
'', -- if(endTime is null, '', Cast(Cast(endTime as Timestamp) as String))
device.physicalDeviceId, -- device.deviceId
device.deviceSourceID,
device.serviceAccountId, -- device.account
device.accountSourceID,
application.applicationName, -- device.appName
application.applicationVersion, -- device.appVersion
device.ipaddress,
NULL, -- device.isp
device.networkLocation,
NULL, -- device.userAgent
NULL, -- asset.programId
asset.mediaGuid,
asset.providerId,
asset.assetId,
asset.assetContentId,
asset.mediaId,
asset.platformId,
asset.recordingId,
asset.streamId,
asset.title,
asset.assetClass, -- assetclass
NULL, -- regulatoryType
sessionDuration,
completionStatus,
asset.playbackType, -- playbackType
if(playbackStarted is not NULL,"Y","N"), -- as playbackStarted
'', -- if(leaseInfo.startOfWindow is null, '', cast(cast(leaseInfo.startOfWindow as Timestamp) as String))
'', -- if(leaseInfo.endOfWindow is null, '', cast(cast(leaseInfo.endOfWindow as Timestamp) as String))
'', -- if(leaseInfo.purchasetime is null, '', cast(cast(leaseInfo.purchasetime as Timestamp) as String))
NULL, -- leaseInfo.leaseLength
NULL, -- leaseInfo.leaseId
NULL, -- leaseInfo.leasePrice
NULL, -- firstAssetPosition
NULL, -- lastAssetPosition
NULL, -- drmLatency
NULL, -- openLatency
'0', -- TRICKPLAYEVENT_PLAY
'0', -- TRICKPLAYEVENT_PAUSE
'0', -- TRICKPLAYEVENT_RWD
'0', -- TRICKPLAYEVENT_FFWD
NULL, -- iptv.concatArrayToString(fragmentwarningevents.offsets, '|')  as fragmentWarningEvents
NULL, -- frameRateMin
NULL, -- frameRateMax
NULL, -- bitRateMin
NULL, -- bitRateMax
NULL, -- ERROREVENTS.OFFSET
NULL, -- errorEvents.message
NULL, -- errorEvents.code
NULL, -- errorEvents.errorType
event_date,
hdp_create_ts,
hdp_update_ts
