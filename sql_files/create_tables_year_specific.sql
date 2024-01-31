CREATE TABLE IF NOT EXISTS ttw.ttw_log_import_mobiele_retroreflectometerYYYY AS
SELECT oid, 'temp/' ||  drive_file_name AS bestands_url, '\' || drive_folder_name || '\' ||  drive_file_name AS map_bestand
	, drive_folder_name || '\' ||  measure_series_name AS bestandsnaam, measure_series_name
	, CASE WHEN name_conform IS TRUE THEN 'bestandsnaam conform header / bestandsmap conform header'
		ELSE 'bestandsnaam NIET conform header / bestandsmap NIET conform header' END AS ctr_header_bestandsnaam
	, datetime_utc_read AS inleesdatum
FROM ttw.log_files_state lfs
WHERE lfs.completed_step = 4 AND report_year = 'YYYY';

CREATE TABLE IF NOT EXISTS ttw.ttw_t_import_mobiele_retroreflectometerYYYY (
    "oid" serial4 NOT NULL,
	bestands_url varchar NULL,
	toestel varchar(100) NULL,
	meting_serie_naam varchar(100) NULL,
	rijstrook varchar(5) NULL,
	toestel_zijde varchar(5) NULL,
	markering varchar(5) NULL,
	locatieomschrijving varchar(100) NULL,
	operator_naam varchar(25) NULL,
	kmp numeric(10, 4) NULL,
	tijdstip_meting_tekst varchar(25) NULL,
	tijdstip_meting timestamp NULL,
	rl int4 NULL,
	rlmin int4 NULL,
	rlmax int4 NULL,
	rlstddev int4 NULL,
	rlprocpass int4 NULL,
	marker varchar(100) NULL,
	temperatuur int4 NULL,
	vochtigheid int4 NULL,
	snelheid numeric(6, 2) NULL,
	"reference" varchar(25) NULL,
	longitude_start numeric(12, 9) NULL,
	latitude_start numeric(12, 9) NULL,
	longitude_eind numeric(12, 9) NULL,
	latitude_eind numeric(12, 9) NULL,
	naam_foto1 varchar(25) NULL,
	naam_foto2 varchar(25) NULL,
	inleesdatum timestamp NULL
);