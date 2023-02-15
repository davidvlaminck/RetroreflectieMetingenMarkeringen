# pip install google-api-python-client
# pip install psycopg2-binary

from pathlib import Path

from DriveWrapper import DriveWrapper
from PostGISConnector import PostGISConnector
from SettingsManager import SettingsManager
from new_scripts.copy_xls_files_to_new_teamdrive import copy_xls_files_to_new_teamdrive


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

    step = 2

    if step <= 1:
        copy_xls_files_to_new_teamdrive(wrapper=wrapper, dir_id_meetjaar_orig=dir_id_meetjaar_orig,
                                        dir_id_root_meetjaar=dir_id_root_meetjaar)

    if step <= 2:
        connector.set_up_tables()

    if step <= 3:

