drop table anchor_segment.anchor_seg_scoring;
create table anchor_segment.anchor_seg_scoring(
ecid                      int,
anchor_segm_score_name    string,
anchor_segm_score         int)
PARTITIONED BY(process_date  date)
TBLPROPERTIES ('parquet.compression'='SNAPPY');
