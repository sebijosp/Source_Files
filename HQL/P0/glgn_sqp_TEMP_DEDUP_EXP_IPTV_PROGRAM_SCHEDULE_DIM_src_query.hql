Select

<SELECT_COLS_PLACEHOLDER> from

( select *, row_number() over (Partition by STATION_ID,AIRING_START_TS order by schedule_window_duration_num asc) rank
from iptv.program_schedule_dim
where airing_dt >= date_sub(current_date, 30) and <SQP_DELTA_WHERE_CONDITION_PLACEHOLDER> and airing_start_ts < airing_end_ts) deduped where rank = 1


