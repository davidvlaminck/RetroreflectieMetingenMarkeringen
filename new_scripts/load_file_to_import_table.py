import csv
import datetime

from PostGISConnector import PostGISConnector


def import_all_files_from_temp_to_table(connector: PostGISConnector, report_year: int):
    connection = connector.get_connection()
    cursor = connection.cursor()
    select_q = f"""SELECT bestands_url 
    FROM ttw.ttw_log_import_mobiele_retroreflectometer{report_year} 
    WHERE ctr_header_bestandsnaam NOT LIKE '%NIET%'"""

    cursor.execute(select_q)
    file_paths = cursor.fetchall()
    for file_path in file_paths:
        csv_path = file_path[0].replace('.xls', '.csv')
        print(f'importing {csv_path}')
        import_file_from_temp_to_table(cursor=cursor, report_year=report_year, file_path=csv_path)
        connection.commit()

    connection.close()


def process_all_imported_files_data(connector: PostGISConnector, report_year: int):
    connection = connector.get_connection()
    create_table_mobiele(connection, report_year)
    create_table_line(connection, report_year)
    fill_table_line(connection, report_year)


def fill_table_line(connection, report_year: int):
    cursor = connection.cursor()
    sql_fill_table_line = f"""  
    insert into ttw.ttw_t_retroreflectometingen_{report_year}_line
    select oid, bestands_url,toestel, meting_serie_naam, marker,markering, tijdstip_meting ,locatieomschrijving, operator_naam, ident8,kmp,	rl,rlmax,rlmin,rlprocpass,rlstddev,vochtigheid,
    CASE 	when left(markering,2) ='M1' and right(ident8,1) ='1' then 2000
    		when left(markering,2) ='M2' and right(ident8,1) ='1' then 1000
            when left(markering,2) ='M1' and right(ident8,1) ='2' then -2000
            when left(markering,2) ='M2' and right(ident8,1) ='2' then -1000
            end as kaart_offset,
    	snelheid,inleesdatum,--(select max(inleesdatum)from ttw.ttw_t_retroreflectometingen_{report_year}_line) as maxinleesdatum,
        ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370)) as geom_line,
        ST_length(ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))) as lengte
        from ttw.ttw_t_mobiele_retroreflectometer{report_year}
    where  (ST_length(ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370)))between 0 and 100)
    		and ( inleesdatum>(select max(inleesdatum)from ttw.ttw_t_retroreflectometingen_{report_year}_line) or ((select count(inleesdatum) from ttw.ttw_t_retroreflectometingen_{report_year}_line) < 1 ));
    """
    cursor.execute(sql_fill_table_line)
    connection.commit()

def create_table_line(connection, report_year: int):
    cursor = connection.cursor()
    sql_create_table_line = f"""
        CREATE TABLE if not exists ttw.ttw_t_retroreflectometingen_{report_year}_line  (
    	"oid" int4 NULL PRIMARY KEY,
    	bestands_url varchar NULL,
    	toestel varchar(100) NULL,
    	meting_serie_naam varchar(100) NULL,
    	marker varchar(100) NULL,
    	markering varchar(5) NULL,
    	tijdstip_meting timestamp NULL,
    	locatieomschrijving varchar(100) NULL,
    	operator_naam varchar(25) NULL,
    	ident8 text NULL,
    	kmp numeric(10, 4) NULL,
    	rl int4 NULL,
    	rlmax int4 NULL,
    	rlmin int4 NULL,
    	rlprocpass int4 NULL,
    	rlstddev int4 NULL,
    	vochtigheid int4 NULL,
    	kaart_offset int4 NULL,
    	snelheid numeric(6, 2) NULL,
    	inleesdatum timestamp NULL,
    	geom_line public.geometry NULL,
    	lengte float8 NULL
    );

    ALTER TABLE ttw.ttw_t_retroreflectometingen_{report_year}_line 
    ADD constraint ttw_t_retroreflectometingen_{report_year}_line_geom_check CHECK (st_srid(geom_line) = 31370);
    """
    cursor.execute(sql_create_table_line)
    connection.commit()


def create_table_mobiele(connection, report_year: int):
    cursor = connection.cursor()
    sql_create_table_mobiele = f"""DROP TABLE IF EXISTS  ttw.ttw_t_mobiele_retroreflectometer{report_year};
    create table if not exists ttw.ttw_t_mobiele_retroreflectometer{report_year} as
    SELECT	oid,bestands_url, toestel, meting_serie_naam,left(meting_serie_naam,8) as ident8,
        markering, tijdstip_meting ,locatieomschrijving, operator_naam, kmp, rl,rlmax,rlmin,rlprocpass,rlstddev,vochtigheid,snelheid,marker,
        latitude_start,longitude_start,latitude_eind,longitude_eind,
    	ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370)as geomL72,
    	ST_X(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))::INT as X_coord,
    	ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))::INT as Y_coord,
    	 concat(latitude_start,',',longitude_start) as start_coord,
         concat(latitude_eind,',',longitude_eind) as eind_coord,
         inleesdatum
    FROM ttw.ttw_t_import_mobiele_retroreflectometer{report_year}
    WHERE date_part('year', tijdstip_meting) = {report_year};
    """
    cursor.execute(sql_create_table_mobiele)
    connection.commit()


def append_pictures(insert_row, row):
    count = 0
    for i in range(16, 22):
        if row[i] != '-':
            count += 1
            if count > 2:
                return
            insert_row.append(f"'{row[i].split('.jpg')[1]}.jpg'")

    while count < 2:
        count += 1
        insert_row.append("'-'")


def import_file_from_temp_to_table(cursor, report_year, file_path):
    with open(file_path, encoding='ISO-8859-1') as csv_file:
        reader = csv.reader(csv_file, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')

        reading_headers = True
        reading_data_header = False
        headers_dict = {}
        insert_data = []
        for row in reader:
            row = [r.replace('"', '').replace(';', '') for r in row]
            if reading_headers and row[0].strip() == '':
                reading_headers = False
                reading_data_header = True
                continue

            if reading_headers:
                headers_dict[row[0]] = row[1]
            elif reading_data_header:
                reading_data_header = False
            else:
                dt_meting = row[1]
                if '/' in dt_meting:
                    dt_meting = datetime.datetime.strptime(dt_meting, '%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                insert_row = [f"'{str(file_path)}'",  # bestands_url
                              f"'{headers_dict['Info:']}'",  # toestel
                              f"'{headers_dict['Measure Series Name:']}'",  # meting_serie_naam
                              f"'{headers_dict['Lane:']}'",  # rijstrook
                              f"'{headers_dict['Side:']}'",  # toestel_zijde
                              f"'{headers_dict['Marking Name:']}'",  # markering
                              f"'{headers_dict['Remarks:']}'",  # locatieomschrijving
                              f"'{headers_dict['Driver Name:']}'",  # operator_naam
                              f"{row[0]}".replace(',', '.'),  # kmp
                              f"'{dt_meting}'",  # tijdstip_meting_tekst
                              f"'{dt_meting}'::timestamp",  # tijdstip_meting
                              f"{row[2]}".replace(',', '.'),  # rl
                              f"{row[3]}".replace(',', '.'),  # rlmin
                              f"{row[4]}".replace(',', '.'),  # rlmax
                              f"{row[5]}".replace(',', '.'),  # rlstddev
                              f"{row[6]}".replace(',', '.'),  # rlprocpass
                              f"'{row[7]}'",  # marker
                              f"{row[8]}".replace(',', '.'),  # temperatuur
                              f"{row[9]}".replace(',', '.'),  # vochtigheid
                              f"{row[10]}".replace(',', '.'),  # snelheid
                              f"'{row[11]}'",  # reference
                              f"{row[12]}".replace(',', '.'),  # longitude_start
                              f"{row[13]}".replace(',', '.'),  # latitude_start
                              f"{row[14]}".replace(',', '.'),  # longitude_eind
                              f"{row[15]}".replace(',', '.')]  # latitude_eind
                append_pictures(insert_row, row)
                insert_row.append(f"'{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'::timestamp")  # inleesdatum
                insert_data.append(insert_row)

        row_strs = [', '.join(row) for row in insert_data]
        values = '), ('.join(row_strs)

        insert_query = f"""WITH t as (VALUES ({values}))
        INSERT INTO ttw.ttw_t_import_mobiele_retroreflectometer{report_year}
        (bestands_url, toestel, meting_serie_naam, rijstrook, toestel_zijde, markering, locatieomschrijving, operator_naam, kmp, tijdstip_meting_tekst, tijdstip_meting, rl, rlmin, rlmax, rlstddev, rlprocpass, marker, temperatuur, vochtigheid, snelheid, reference, longitude_start, latitude_start, longitude_eind, latitude_eind, naam_foto1, naam_foto2, inleesdatum)
        SELECT * FROM t;"""

        cursor.execute(insert_query)
