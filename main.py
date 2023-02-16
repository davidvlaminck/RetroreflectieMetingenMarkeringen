# pip install google-api-python-client
# pip install psycopg2-binary
# pip install zipfile
import datetime
import os
import zipfile
from pathlib import Path

from DriveWrapper import DriveWrapper
from PostGISConnector import PostGISConnector
from SettingsManager import SettingsManager
from new_scripts.copy_xls_files_to_new_teamdrive import copy_xls_files_to_new_teamdrive


def load_files_to_log_table(connector: PostGISConnector, wrapper: DriveWrapper, root_meetjaar, report_year: int):
    dir_name = 'Te_verwerken'
    dir_id = wrapper.find_directory_by_name(directory_name=dir_name, directory_id=root_meetjaar)[0]

    insert_timestamp = datetime.datetime.utcnow()
    for file in wrapper.list_files_in_directory(dir_id, recursive_level=1, file_types=['xls']):
        insert_query = f"""
        WITH t (drive_file_name, drive_file_id, drive_folder_name, drive_folder_id, datetime_utc_read, completed_step, report_year) 
            AS (VALUES ('{file['name']}', '{file['id']}', '{file['parent_name']}', '{file['parent_id']}', 
                '{insert_timestamp}'::timestamp, 3, {report_year})),
        to_insert AS (
            SELECT t.* 
            FROM t
                LEFT JOIN ttw.log_files_state AS log_files_state ON 
                    log_files_state.drive_file_name = t.drive_file_name AND log_files_state.report_year = t.report_year
            WHERE log_files_state.drive_file_id IS NULL)
        INSERT INTO ttw.log_files_state (drive_file_name, drive_file_id, drive_folder_name, drive_folder_id, 
            datetime_utc_read, completed_step, report_year)
        SELECT to_insert.drive_file_name, to_insert.drive_file_id, to_insert.drive_folder_name, 
            to_insert.drive_folder_id, to_insert.datetime_utc_read, to_insert.completed_step, to_insert.report_year
        FROM to_insert;"""
        print(file)
        connector.execute_query(insert_query)


def download_and_process_file(connector, wrapper, root_meetjaar, report_year):
    with connector.main_connection.cursor() as cursor:
        while True:
            cursor.execute(f'SELECT * FROM ttw.log_files_state WHERE report_year = {report_year} AND '
                           f'name_conform IS NULL AND completed_step = 3 LIMIT 1;')
            record = cursor.fetchone()
            if record is None:
                break
            file_id = record[2]
            file_name = record[1]
            wrapper.download_file(file_id=file_id, file_path=Path(f'temp/{file_name}'))
            print(record)

            if not os.path.exists(Path('temp')):
                os.mkdir(Path('temp'))
            if not os.path.exists(Path('wdb')):
                os.mkdir(Path('wdb'))

            csv_path = Path(f'temp/{file_name}'.replace('.xls', '.csv'))
            import csv
            with open(Path(f'temp/{file_name}'), encoding='ISO-8859-1') as csv_file:
                with open(csv_path, 'w+', encoding='utf-8') as output_file:
                    reader = csv.reader(csv_file, delimiter='\t', quoting=csv.QUOTE_NONE, escapechar='\\')
                    writer = csv.writer(output_file, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')

                    reading_headers = True
                    headers_dict = {}
                    data = []
                    for row in reader:
                        row = [r.replace('"', '').replace(';', '').replace('Temperature °C', 'Temperature Â°C') for r in row]
                        writer.writerow(row)
                        if reading_headers and row[0].strip() == '':
                            reading_headers = False
                            continue

                        if reading_headers:
                            headers_dict[row[0]] = row[1]
                        else:
                            data.append(row)

                    data_headers = data[0]

                    measure_series_name = headers_dict['Measure Series Name:']

                    name_conform = True
                    filename = record[1][0:-4]
                    if filename != measure_series_name:
                        name_conform = False
                    first_part = measure_series_name.split(' ')[0]
                    if first_part != record[3]:
                        name_conform = False

                    if not name_conform:
                        print(f'name not conform: {measure_series_name}, {record[1]}, {record[3]}')

                    update_query = f"UPDATE ttw.log_files_state " \
                                   f"SET name_conform = {name_conform}, measure_series_name = '{measure_series_name}', " \
                                   f"completed_step = 4 " \
                                   f"WHERE oid = {record[0]}"
                    cursor.execute(update_query)

                    # TODO import data in ttw_t_import_mobiele_retroreflectometer

                    zip_path = Path(f'wdb/{file_name}'.replace('.xls', '.zip'))
                    zipfile.ZipFile(zip_path, mode='w').write(csv_path, arcname=csv_path.name)

                    connector.main_connection.commit()


if __name__ == '__main__':
    settings_manager = SettingsManager(settings_path='settings_evt2.json')
    db_settings = settings_manager.settings['databases']['prd']
    connector = PostGISConnector(**db_settings)

    wrapper = DriveWrapper(
        service_cred_path=Path('/home/davidlinux/Documents/AWV/resources/driven-wonder-149715-ca8bdf010930.json'),
        readonly_scope=False)

    # Meetjaar 20xx map op de Gedeelde Drive "Systematische Retroreflectiemetingen"
    dir_id_meetjaar_orig = '1Me7ND5IpRTrVtXQjrfopjEsaVaxwtDqu'

    # Nieuwe Meetjaar 20xx map op de Gedeelde Drive specifiek voor dat jaar
    dir_id_root_meetjaar = '1bGRYh6dcvFoC5tQP92NspPpTB9743DaB'

    report_year = 2022

    step = 4

    if step <= 1:
        copy_xls_files_to_new_teamdrive(wrapper=wrapper, dir_id_meetjaar_orig=dir_id_meetjaar_orig,
                                        dir_id_root_meetjaar=dir_id_root_meetjaar)

    if step <= 2:
        connector.set_up_tables()

    if step <= 3:
        load_files_to_log_table(connector=connector, wrapper=wrapper, root_meetjaar=dir_id_root_meetjaar,
                                report_year=report_year)

    if step <= 4:
        download_and_process_file(connector=connector, wrapper=wrapper, root_meetjaar=dir_id_root_meetjaar,
                                  report_year=report_year)
