CREATE VIEW iptv.vw_ltv_str_recon_stg AS
 select ltv_str_recon_stg.uuid, ltv_str_recon_stg.rogers_account_id, ltv_str_recon_stg.comcast_account_id, ltv_str_recon_stg.rogers_device_id, ltv_str_recon_stg.comcast_device_id, ltv_str_recon_stg.comcast_session_id, ltv_str_recon_stg.session_start_ts_utc, ltv_str_recon_stg.session_end_ts_utc, ltv_str_recon_stg.app_name, ltv_str_recon_stg.app_version_num, ltv_str_recon_stg.ipaddress_num, ltv_str_recon_stg.isp_name, ltv_str_recon_stg.network_location_name, ltv_str_recon_stg.user_agent_name, ltv_str_recon_stg.asset_id, ltv_str_recon_stg.asset_class_cd, ltv_str_recon_stg.media_guid, ltv_str_recon_stg.provider_id, ltv_str_recon_stg.asset_content_id, ltv_str_recon_stg.program_id, ltv_str_recon_stg.merlin_recording_id, ltv_str_recon_stg.merlin_stream_id, ltv_str_recon_stg.compass_stream_id, ltv_str_recon_stg.session_duration_count, ltv_str_recon_stg.completion_status_cd, ltv_str_recon_stg.playback_type_cd, ltv_str_recon_stg.playback_started_flag, ltv_str_recon_stg.first_asset_position_num, ltv_str_recon_stg.last_asset_position_num, ltv_str_recon_stg.drm_latency_num, ltv_str_recon_stg.open_latency_num, ltv_str_recon_stg.trickplay_events_num, ltv_str_recon_stg.fragment_warning_events_num, ltv_str_recon_stg.frame_rate_min_num, ltv_str_recon_stg.frame_rate_max_num, ltv_str_recon_stg.bit_rate_min_num, ltv_str_recon_stg.bit_rate_max_num, ltv_str_recon_stg.hdp_create_ts, ltv_str_recon_stg.hdp_update_ts, ltv_str_recon_stg.channel_sk, ltv_str_recon_stg.subscriber_key, ltv_str_recon_stg.subscriber_no, ltv_str_recon_stg.subscriber_seq_no, ltv_str_recon_stg.customer_key, ltv_str_recon_stg.customer_id, ltv_str_recon_stg.address_key, ltv_str_recon_stg.data_mac_id, ltv_str_recon_stg.error_code, ltv_str_recon_stg.first_create_dt, ltv_str_recon_stg.session_start_date_id, ltv_str_recon_stg.session_start_time_id, ltv_str_recon_stg.session_start_ts_localtime, ltv_str_recon_stg.session_end_ts_localtime from IPTV.LTV_STR_RECON_STG
;
