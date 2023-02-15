CREATE TABLE IF NOT EXISTS ttw.log_files_state (
	"oid" serial4 NOT NULL,
	drive_file_id varchar NULL,
	drive_folder_id varchar NULL,
	file_name varchar NULL,
	measure_series_name varchar NULL,
	datetime_utc_read timestamp NULL,
	name_conform bool NULL,
	step int4 NULL,
	report_year int4 NULL
);