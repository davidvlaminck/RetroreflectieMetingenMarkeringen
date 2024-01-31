import csv
import datetime
import logging
import os
from pathlib import Path

from PostGISConnector import PostGISConnector

example_data = [['oid', 'bestands_url', 'toestel', 'meting_serie_naam', 'rijstrook', 'toestel_zijde', 'markering',
                 'locatieomschrijving', 'operator_naam', 'kmp', 'tijdstip_meting_tekst', 'tijdstip_meting', 'rl',
                 'rlmin', 'rlmax', 'rlstddev', 'rlprocpass', 'marker', 'temperatuur', 'vochtigheid', 'snelheid',
                 'reference', 'longitude_start', 'latitude_start', 'longitude_eind', 'latitude_eind', 'naam_foto1',
                 'naam_foto2', 'inleesdatum'],
                ['8,928',
                 'H:\Gedeelde drives\Systematische Retroreflectiemetingen\Meetjaar 2021\A0120001_M2\A0120001_M2 21.6 - 23.2.xls',
                 'Zehntner ZDR6020 S/N 466020979', 'A0120001_M2 21.6 - 23.2', '2', 'L', 'M2',
                 'A12 BRUSSEL - ANTWERPEN MP 21.6 - 23.2 M2', 'Kevin VdH', '22.8', '20/04/2021 10:58:50',
                 '2021-04-20 10:58:50.000', '92', '16', '116', '23', '60', '', '16', '49', '85.66', '', '0', '0', '0',
                 '0', '-', '-', '2021-11-23 12:59:59.000']]


def create_extra_tables(cursor, report_year):
    # sql_create_table = """create table if not exists ttw_t_mobiele_retroreflectometer_""" + str(meetjaar) + """ as
    # SELECT	oid,bestands_url, toestel, meting_serie_naam,left(meting_serie_naam,8) as ident8,
    #     markering, tijdstip_meting ,locatieomschrijving, operator_naam, kmp, rl,rlmax,rlmin,rlprocpass,rlstddev,vochtigheid,snelheid,marker,
    #     latitude_start,longitude_start,latitude_eind,longitude_eind,
    # 	ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370)as geomL72,
    # 	ST_X(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))::INT as X_coord,
    # 	ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))::INT as Y_coord,
    # 	 concat(latitude_start,',',longitude_start) as start_coord,
    #      concat(latitude_eind,',',longitude_eind) as eind_coord,
    #      inleesdatum
    # FROM """ + str(PostgisTabelName) + """
    # where date_part('year',tijdstip_meting) = """ + str(meetjaar) + """;
    # """

    # sql_make_line = """create table if not exists ttw.ttw_t_retroreflectometingen_""" + str(meetjaar) + """_line as
    # select oid,bestands_url,toestel, meting_serie_naam, marker,markering, tijdstip_meting ,locatieomschrijving, operator_naam, ident8,kmp,	rl,rlmax,rlmin,rlprocpass,rlstddev,vochtigheid,
    # CASE when left(markering,2) ='M1' and right(ident8,1) ='1' then 2000
    # 		when left(markering,2) ='M2' and right(ident8,1) ='1' then 1000
    #         when left(markering,2) ='M1' and right(ident8,1) ='2' then -2000
    #         when left(markering,2) ='M2' and right(ident8,1) ='2' then -1000
    #         end as kaart_offset,
    # 	snelheid,inleesdatum,
    #     ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370)) as geom_line,
    #     ST_length(ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))) as lengte
    #     from ttw_t_mobiele_retroreflectometer_""" + str(meetjaar) + """
    # where  1=2;
    #
    # --alter table ttw_t_retroreflectometingen_""" + str(
    #     meetjaar) + """_line ADD constraint ttw_t_retroreflectometingen_""" + str(meetjaar) + """_line_pkey primary key(oid);
    #
    # --alter table ttw_t_retroreflectometingen_""" + str(
    #     meetjaar) + """_line ADD constraint ttw_t_retroreflectometingen_""" + str(meetjaar) + """_line_geom_check CHECK (st_srid(geom) = 31370);
    #
    # insert into ttw.ttw_t_retroreflectometingen_""" + str(meetjaar) + """_line
    # select oid, bestands_url,toestel, meting_serie_naam, marker,markering, tijdstip_meting ,locatieomschrijving, operator_naam, ident8,kmp,	rl,rlmax,rlmin,rlprocpass,rlstddev,vochtigheid,
    # CASE 	when left(markering,2) ='M1' and right(ident8,1) ='1' then 2000
    # 		when left(markering,2) ='M2' and right(ident8,1) ='1' then 1000
    #         when left(markering,2) ='M1' and right(ident8,1) ='2' then -2000
    #         when left(markering,2) ='M2' and right(ident8,1) ='2' then -1000
    #         end as kaart_offset,
    # 	snelheid,inleesdatum,--(select max(inleesdatum)from ttw_t_retroreflectometingen_""" + str(meetjaar) + """_line) as maxinleesdatum,
    #     ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370)) as geom_line,
    #     ST_length(ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))) as lengte
    #     from ttw_t_mobiele_retroreflectometer_""" + str(meetjaar) + """
    # where  (ST_length(ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370)))between 0 and 100)
    # 		and ( inleesdatum>(select max(inleesdatum)from ttw_t_retroreflectometingen_""" + str(
    #     meetjaar) + """_line) or ((select count(inleesdatum) from ttw_t_retroreflectometingen_""" + str(meetjaar) + """_line) < 1 ));
    # """


def import_all_files_from_temp_to_table(connector: PostGISConnector, report_year: int):
    connection = connector.get_connection()
    cursor = connection.cursor()
    select_q = """SELECT bestands_url 
    FROM ttw.ttw_log_import_mobiele_retroreflectometer2022 
    WHERE ctr_header_bestandsnaam NOT LIKE '%NIET%'"""

    cursor.execute(select_q)
    file_paths = cursor.fetchall()
    for file_path in file_paths:
        csv_path = file_path[0].replace('.xls', '.csv')
        print(f'importing {csv_path}')
        import_file_from_temp_to_table(cursor=cursor, report_year=report_year, file_path=csv_path)
        connection.commit()

    create_extra_tables(cursor=cursor, report_year=report_year)


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
                data_headers = row
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
        INSERT INTO ttw.ttw_t_import_mobiele_retroreflectometerYYYY
        (bestands_url, toestel, meting_serie_naam, rijstrook, toestel_zijde, markering, locatieomschrijving, operator_naam, kmp, tijdstip_meting_tekst, tijdstip_meting, rl, rlmin, rlmax, rlstddev, rlprocpass, marker, temperatuur, vochtigheid, snelheid, reference, longitude_start, latitude_start, longitude_eind, latitude_eind, naam_foto1, naam_foto2, inleesdatum)
        SELECT * FROM t;""".replace('YYYY', str(report_year))

        cursor.execute(insert_query)



#
# import csv
#
# with open(Path(f'temp/{file_name}'), encoding='ISO-8859-1') as csv_file:
#     with open(csv_path, 'w+', encoding='ISO-8859-1') as output_file:
#         reader = csv.reader(csv_file, delimiter='\t', quoting=csv.QUOTE_NONE, escapechar='\\')
#         writer = csv.writer(output_file, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')
#
#         reading_headers = True
#         headers_dict = {}
#         data = []
#         for row in reader:
#             row = [r.replace('"', '').replace(';', '') for r in row]
#             writer.writerow(row)
#             if reading_headers and row[0].strip() == '':
#                 reading_headers = False
#                 continue
#
#             if reading_headers:
#                 headers_dict[row[0]] = row[1]
#             else:
#                 data.append(row)
#
#         data_headers = data[0]
#
#         measure_series_name = headers_dict['Measure Series Name:']
#
#         name_conform = True
#         filename = record[1][:-4]
#         if filename != measure_series_name:
#             name_conform = False
#         first_part = measure_series_name.split(' ')[0]
#         if first_part != record[3]:
#             name_conform = False
#
#         if not name_conform:
#             print(f'name not conform: {measure_series_name}, {record[1]}, {record[3]}')
#
#         update_query = f"UPDATE ttw.log_files_state " \
#                        f"SET name_conform = {name_conform}, measure_series_name = '{measure_series_name}', " \
#                        f"completed_step = 4 " \
#                        f"WHERE oid = {record[0]}"
#         cursor.execute(update_query)
