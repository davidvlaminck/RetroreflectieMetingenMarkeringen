import os
from pathlib import Path

from httplib2 import ServerNotFoundError

from DriveWrapper import DriveWrapper
from PostGISConnector import PostGISConnector
from openpyxl import Workbook

FILE_DIR = Path(__file__).parent


def copy_tables_to_drive(wrapper: DriveWrapper, connector: PostGISConnector, report_year: int,
                         dir_id_root_meetjaar: str):
    excel_path = Path(FILE_DIR.parent / f'temp/analyse_RMM_{report_year}.xlsx')
    os.unlink(excel_path)

    tables = [f'ttw_t_retroreflectometingen_{report_year}_per_district_wegcat',
              f'ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_m',
              f'ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H',
              f'ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_H',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_H_m1',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_H_m2',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_P',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_P_m1',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_P_m2',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_M1',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_M2',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_hoofdweg',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_hoofdweg_M1',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_hoofdweg_M2',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_P1P2weg',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_P1P2weg_M1',
              f'ttw_t_retroreflectometingen_{report_year}_procent_per_prov_P1P2weg_M2']

    wb = Workbook()
    for table in tables:
        ws1 = wb.create_sheet(table.replace(f'ttw_t_retroreflectometingen_{report_year}_', ''))
        cursor = connector.main_connection.cursor()
        cursor.execute(f'SELECT * FROM ttw.{table}')
        rows = cursor.fetchall()
        ws1.append([desc[0] for desc in cursor.description])
        for row in rows:
            ws1.append(row)
    wb.remove(wb['Sheet'])
    wb.save(excel_path)

    try:
        wrapper.upload_file(file_path=excel_path, dir_id=dir_id_root_meetjaar)
    except ServerNotFoundError:
        logging.error('Server not found, could not upload the file to the drive.')


    # create XLS with sheets for each table
    # copy the xls file to drive
    # https://openpyxl.readthedocs.io/en/stable/tutorial.html
    # https://openpyxl.readthedocs.io/en/stable/usage.html
    # raise NotImplementedError
