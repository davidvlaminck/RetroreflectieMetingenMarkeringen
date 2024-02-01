# pip install google-api-python-client
# pip install psycopg2-binary
# pip install zipfile
from pathlib import Path

from DriveWrapper import DriveWrapper
from PostGISConnector import PostGISConnector
from SettingsManager import SettingsManager
from new_scripts.copy_xls_files_to_new_teamdrive import copy_xls_files_to_new_teamdrive
from new_scripts.download_and_process_files import download_and_process_files
from new_scripts.load_beheersegmenten_to_table import create_and_fill_by_import
from new_scripts.load_file_to_import_table import import_all_files_from_temp_to_table
from new_scripts.load_files_to_log_table import load_files_to_log_table

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

    step = 8

    connection = connector.main_connection
    cursor = connection.cursor()
    cursor.execute('SET search_path TO ttw;')

    if step <= 1:
        copy_xls_files_to_new_teamdrive(wrapper=wrapper, dir_id_meetjaar_orig=dir_id_meetjaar_orig,
                                        dir_id_root_meetjaar=dir_id_root_meetjaar)

    if step <= 2:
        connector.set_up_tables()

    if step <= 3:
        load_files_to_log_table(connector=connector, wrapper=wrapper, root_meetjaar=dir_id_root_meetjaar,
                                report_year=report_year)

    if step <= 4:
        download_and_process_files(connector=connector, wrapper=wrapper, root_meetjaar=dir_id_root_meetjaar,
                                   report_year=report_year)

    if step <= 5:
        # manual step: upload files to WDB and have them processed
        print('did you do the manual step?')
        exit()

    if step <= 6:
        connector.create_additional_tables_by_year(report_year=report_year)

    if step <= 7:
        import_all_files_from_temp_to_table(connector=connector, report_year=report_year)

    if step <= 8:
        create_and_fill_by_import(connector=connector, beheersegmenten_year=2024,
                                  file_path=Path('temp/beheersegmenten202402.csv'))
