alter table mt_pricing.fido_payer_segmentation DROP IF EXISTS PARTITION(extraction_date > '2022-06-18',extraction_date < '2022-06-25');
