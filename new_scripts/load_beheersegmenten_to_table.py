import csv
from pathlib import Path

from PostGISConnector import PostGISConnector


def create_and_fill_by_import(connector: PostGISConnector, beheersegmenten_year: int, file_path: Path):
    connection = connector.get_connection()
    create_table(connection=connection, beheersegmenten_year=beheersegmenten_year)

    import_file_from_temp_to_table(cursor=connection.cursor(), beheersegmenten_year=beheersegmenten_year,
                                   file_path=file_path)

    alter_table(connection=connection, beheersegmenten_year=beheersegmenten_year)

    connection.commit()


def alter_table(connection, beheersegmenten_year: int):
    cursor = connection.cursor()
    sql_alter_table_beheersegmenten = f"""
    alter table ttw.gt_beheersegmenten_{beheersegmenten_year} add column district character varying (10);
    update ttw.gt_beheersegmenten_{beheersegmenten_year} set district = right(gebied,6) where eigenbeheer = 'true';

    alter table ttw.gt_beheersegmenten_{beheersegmenten_year} add column provincie character varying(25);
    update ttw.gt_beheersegmenten_{beheersegmenten_year} set provincie =
        CASE WHEN left(right(gebied,3),1)='1' THEN 'Antwerpen'
            WHEN left(right(gebied,3),1)='2' THEN 'Vlaams-Brabant'
            WHEN left(right(gebied,3),1)='3' THEN 'West-Vlaanderen'
            WHEN left(right(gebied,3),1)='4' THEN 'Oost-Vlaanderen'
            WHEN left(right(gebied,3),1)='7' THEN 'Limburg'	END 
    where eigenbeheer = 'true';
    """
    cursor.execute(sql_alter_table_beheersegmenten)
    connection.commit()


def create_table(connection, beheersegmenten_year: int):
    cursor = connection.cursor()
    sql_create_table_beheersegmenten = f"""
    CREATE TABLE ttw.gt_beheersegmenten_{beheersegmenten_year} (
        objectid serial4 NOT NULL,
        id int4 NULL,
        ident8 varchar(8) NULL,
        beginopschrift numeric(38, 8) NULL,
        beginafstand numeric(38, 8) NULL,
        beginpositie numeric(38, 8) NULL,
        eindopschrift numeric(38, 8) NULL,
        eindafstand numeric(38, 8) NULL,
        eindpositie numeric(38, 8) NULL,
        gebied varchar(100) NULL,
        eigenbeheer varchar(250) NULL,
        opmerking varchar(250) NULL,
        lengte numeric(38, 8) NULL,
        werkelijkelengte numeric(38, 8) NULL,
        gebruiker varchar(40) NULL,
        opnamedatum timestamp NULL,
        begindatum timestamp NULL,
        creatiedatum timestamp NULL,
        wijzigingsdatum timestamp NULL,
        deltawijzigingsdatum numeric(38, 8) NULL,
        bronid int4 NULL,
        wegcategorie varchar(100) NULL,
        offset_ int4 NULL,
        copydatum timestamp NULL,
        shape public.geometry NULL,
        CONSTRAINT enforce_srid_shape_1 CHECK ((st_srid(shape) = 31370)),
        CONSTRAINT gt_beheersegmenten_{beheersegmenten_year}_pkey_1 PRIMARY KEY (objectid)
    );
    CREATE INDEX sidx_215004_25_1 ON ttw.gt_beheersegmenten_{beheersegmenten_year} USING btree (shape);
"""
    cursor.execute(sql_create_table_beheersegmenten)
    connection.commit()


def import_file_from_temp_to_table(cursor, beheersegmenten_year: int, file_path: Path):
    with open(file_path, encoding='utf-8') as csv_file:
        reader = csv.reader(csv_file, delimiter='\t', escapechar='\\')

        reading_data_header = True
        insert_data = []
        for row in reader:
            if reading_data_header:
                reading_data_header = False
                data_headers = row
            else:
                insert_row = [row[1],  # id
                              f"'{row[2]}'",  # ident8
                              row[3],  # beginopschrift
                              row[4],  # beginafstand
                              row[5],  # beginpositie
                              row[6],  # eindopschrift
                              row[7],  # eindafstand
                              row[8],  # eindpositie
                              f"'{row[9]}'",  # gebied
                              f"'{row[10]}'",  # eigenbeheer
                              f"'{row[11]}'",  # opmerking
                              row[12],  # lengte
                              row[13],  # werkelijkelengte
                              f"'{row[14]}'",  # gebruiker
                              f"'{row[15]}'",  # opnamedatum
                              f"'{row[16]}'",  # begindatum
                              f"'{row[17]}'",  # creatiedatum
                              f"'{row[18]}'", # wijzigingsdatum
                              row[19],  # deltawijzigingsdatum
                              row[20],  # bronid
                              f"'{row[21]}'", # wegcategorie
                              row[22],  # offset_
                              rf"'{row[23]}'",  # copydatum     ]
                              ]
                insert_data.append(insert_row)

        row_strs = [', '.join(row) for row in insert_data]
        values = '), ('.join(row_strs)

        insert_query = f"""INSERT INTO ttw.gt_beheersegmenten_{beheersegmenten_year}
(id, ident8, beginopschrift, beginafstand, beginpositie, eindopschrift, eindafstand, eindpositie, gebied, eigenbeheer, opmerking, lengte, werkelijkelengte, gebruiker, opnamedatum, begindatum, creatiedatum, wijzigingsdatum, deltawijzigingsdatum, bronid, wegcategorie, offset_, copydatum)
VALUES({values});"""

        cursor.execute(insert_query)
